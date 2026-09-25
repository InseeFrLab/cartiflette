"""
Production pipeline: the functions called by the Argo workflow steps.

1. `prepare_year`  -> inputs of a year (see cartiflette.prepare)
2. `combinations`  -> list of (level, filter_by, simplification) jobs
3. `split_and_upload` -> one job: mapshaper processing, GeoJSON + GeoParquet
   files written to S3
"""

from __future__ import annotations

import json
import logging
import os
import shutil

import duckdb
import s3fs

from cartiflette import config, mapshaper
from cartiflette.paths import create_path_bucket
from cartiflette.prepare import prepare_year  # noqa: F401 (pipeline step)
from cartiflette.s3 import check_write_target, upload

logger = logging.getLogger(__name__)

# Levels of the polygons -> levels used to split the files
SPLITS = {
    "COMMUNE": [
        "BASSIN_VIE",
        "ZONE_EMPLOI",
        "UNITE_URBAINE",
        "AIRE_ATTRACTION_VILLES",
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "DEPARTEMENT": [
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "REGION": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "BASSIN_VIE": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "ZONE_EMPLOI": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "UNITE_URBAINE": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "AIRE_ATTRACTION_VILLES": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
}
# Communes are also produced with Paris, Lyon, Marseille split by arrondissement
SPLITS["COMMUNE_ARRONDISSEMENT"] = SPLITS["COMMUNE"]

SIMPLIFICATIONS = [0, 50]
CRS = [4326]


def combinations(
    levels: list[str] | None = None,
    simplifications: list[float] = SIMPLIFICATIONS,
    crs_list: list[int] = CRS,
) -> list[dict]:
    """All the jobs to run, optionally restricted to some polygon levels."""
    return [
        {
            "level_polygons": level,
            "filter_by": filter_by,
            "simplification": simplification,
            "crs": crs,
        }
        for level, filters in SPLITS.items()
        if levels is None or level in levels
        for filter_by in filters
        for simplification in simplifications
        for crs in crs_list
    ]


def geojson_to_parquet(geojson_path: str, parquet_path: str) -> str:
    """
    Convert a GeoJSON file to GeoParquet with DuckDB. ST_Read (GDAL) is run
    single-threaded, see cartiflette.ign.gpkg_to_parquet.
    """
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false; SET threads = 1;")
        con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true;")
        con.execute(
            "COPY (SELECT * EXCLUDE (OGC_FID, geom), geom AS geometry "
            f"FROM ST_Read('{geojson_path}')) "
            f"TO '{parquet_path}' (FORMAT parquet)"
        )
    return parquet_path


def process(
    inputs_dir: str,
    work_dir: str,
    level_polygons: str,
    filter_by: str,
    simplification: float,
    crs: int,
) -> list[str]:
    """
    Produce the GeoJSON files of one job from the prepared inputs: dissolve
    communes if needed, bring the DROM closer if needed, then simplify and
    split. Returns the paths of the files, named `{value}.geojson`.
    """
    with open(os.path.join(inputs_dir, "fields.json")) as f:
        fields = mapshaper.level_fields(json.load(f))

    os.makedirs(work_dir, exist_ok=True)
    base_level = (
        "COMMUNE_ARRONDISSEMENT" if level_polygons == "COMMUNE_ARRONDISSEMENT" else "COMMUNE"
    )
    current = os.path.join(inputs_dir, f"{base_level}.geojson")

    if level_polygons != base_level:
        current = mapshaper.dissolve(
            current,
            os.path.join(work_dir, "dissolved.geojson"),
            level_polygons,
            filter_by,
            fields,
        )

    if filter_by == "FRANCE_ENTIERE_DROM_RAPPROCHES":
        current = mapshaper.bring_drom_closer(
            current,
            os.path.join(work_dir, "drom_rapproches.geojson"),
            level_polygons,
            fields,
        )

    return mapshaper.split(
        current,
        os.path.join(work_dir, "split"),
        fields[filter_by],
        crs,
        simplification,
        f"{config.PROVIDER}:{config.SOURCE}",
    )


def split_and_upload(
    year: int,
    inputs_dir: str,
    work_dir: str,
    level_polygons: str,
    filter_by: str,
    simplification: float,
    crs: int,
    fs: s3fs.S3FileSystem,
    bucket: str = config.WRITE_BUCKET,
    path_within_bucket: str = config.WRITE_PATH,
) -> list[str]:
    """Run one job and upload its GeoJSON and GeoParquet files to S3."""
    # Fail before any processing if the target is not allowed
    check_write_target(bucket, path_within_bucket)

    geojsons = process(
        inputs_dir, work_dir, level_polygons, filter_by, simplification, crs
    )
    uploaded = []
    for geojson in geojsons:
        value = os.path.basename(geojson).removesuffix(".geojson")
        local = {
            "geojson": geojson,
            "parquet": geojson_to_parquet(geojson, geojson.replace(".geojson", ".parquet")),
        }
        for vectorfile_format in config.OUTPUT_FORMATS:
            remote = create_path_bucket(
                bucket=bucket,
                path_within_bucket=path_within_bucket,
                provider=config.PROVIDER,
                dataset_family=config.DATASET_FAMILY,
                source=config.SOURCE,
                year=year,
                borders=level_polygons,
                crs=crs,
                filter_by=filter_by,
                value=value,
                vectorfile_format=vectorfile_format,
                territory=config.TERRITORY,
                simplification=simplification,
            )
            uploaded.append(upload(local[vectorfile_format], remote, fs))

    shutil.rmtree(work_dir)
    return uploaded
