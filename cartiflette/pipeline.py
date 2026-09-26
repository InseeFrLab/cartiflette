"""
Production pipeline: the functions called by the Argo workflow steps.

1. `prepare_year` -> inputs of a year (see cartiflette.prepare)
2. GeoJSON, one file per value of a split level:
   `combinations` lists the (level, filter_by, simplification, crs) jobs,
   `split_and_upload` runs one of them.
3. GeoParquet, one consolidated file per level and layout, filtered when
   read: `consolidated_combinations` lists the (level, layout,
   simplification, crs) jobs, `consolidate_and_upload` runs one of them.
"""

from __future__ import annotations

import json
import logging
import os
import shutil

import duckdb
import s3fs

from cartiflette import config, mapshaper
from cartiflette.paths import create_path_bucket, create_path_consolidated
from cartiflette.prepare import prepare_year  # noqa: F401 (pipeline step)
from cartiflette.s3 import check_write_target, upload

logger = logging.getLogger(__name__)

DROM_RAPPROCHES = "FRANCE_ENTIERE_DROM_RAPPROCHES"

# Levels of the polygons -> levels used to split (GeoJSON) or filter
# (GeoParquet) the files
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
        DROM_RAPPROCHES,
    ],
    "DEPARTEMENT": ["REGION", "TERRITOIRE", "FRANCE_ENTIERE", DROM_RAPPROCHES],
    "REGION": ["TERRITOIRE", "FRANCE_ENTIERE", DROM_RAPPROCHES],
    "BASSIN_VIE": ["TERRITOIRE", "FRANCE_ENTIERE", DROM_RAPPROCHES],
    "ZONE_EMPLOI": ["TERRITOIRE", "FRANCE_ENTIERE", DROM_RAPPROCHES],
    "UNITE_URBAINE": ["TERRITOIRE", "FRANCE_ENTIERE", DROM_RAPPROCHES],
    "AIRE_ATTRACTION_VILLES": ["TERRITOIRE", "FRANCE_ENTIERE", DROM_RAPPROCHES],
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
    """GeoJSON jobs, optionally restricted to some polygon levels."""
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


def consolidated_combinations(
    levels: list[str] | None = None,
    simplifications: list[float] = SIMPLIFICATIONS,
    crs_list: list[int] = CRS,
) -> list[dict]:
    """GeoParquet jobs, optionally restricted to some polygon levels."""
    return [
        {
            "level_polygons": level,
            "layout": layout,
            "simplification": simplification,
            "crs": crs,
        }
        for level in SPLITS
        if levels is None or level in levels
        for layout in config.LAYOUTS
        for simplification in simplifications
        for crs in crs_list
    ]


def filter_levels(level_polygons: str, layout: str) -> list[str]:
    """Levels a consolidated file of `layout` can be filtered by."""
    if layout == DROM_RAPPROCHES:
        return [DROM_RAPPROCHES]
    return [x for x in SPLITS[level_polygons] if x != DROM_RAPPROCHES]


def sort_levels(level_polygons: str, levels: list[str]) -> list[str]:
    """
    Levels a consolidated file is sorted by.

    Nested administrative levels first (region, departement), then, for
    communes, the living area (the smallest zoning, made of neighbouring
    communes). The rows of a region, a departement or a zoning are then
    contiguous and fit in one or two row groups, which the clients read
    alone. Without administrative level (zonings), the territory comes
    first.
    """
    available = {level_polygons, *levels}
    order = [x for x in ("REGION", "DEPARTEMENT") if x in available]
    if not order and "TERRITOIRE" in available:
        order = ["TERRITOIRE"]
    if level_polygons.startswith("COMMUNE") and "BASSIN_VIE" in available:
        order.append("BASSIN_VIE")
    return list(dict.fromkeys([*order, level_polygons]))


def _read_fields(inputs_dir: str) -> dict[str, str]:
    with open(os.path.join(inputs_dir, "fields.json")) as f:
        return mapshaper.level_fields(json.load(f))


def _prepare_polygons(
    inputs_dir: str,
    work_dir: str,
    level_polygons: str,
    keep_levels: list[str],
    drom_rapproches: bool,
    fields: dict[str, str],
) -> str:
    """
    Polygons of `level_polygons` (communes dissolved if needed), keeping the
    fields of `keep_levels`, with the DROM brought closer if requested.
    """
    os.makedirs(work_dir, exist_ok=True)
    base_level = (
        "COMMUNE_ARRONDISSEMENT"
        if level_polygons == "COMMUNE_ARRONDISSEMENT"
        else "COMMUNE"
    )
    current = os.path.join(inputs_dir, f"{base_level}.geojson")

    if level_polygons != base_level:
        current = mapshaper.dissolve(
            current,
            os.path.join(work_dir, "dissolved.geojson"),
            level_polygons,
            keep_levels,
            fields,
        )

    if drom_rapproches:
        current = mapshaper.bring_drom_closer(
            current,
            os.path.join(work_dir, "drom_rapproches.geojson"),
            level_polygons,
            fields,
        )
    return current


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
    fields = _read_fields(inputs_dir)
    polygons = _prepare_polygons(
        inputs_dir,
        work_dir,
        level_polygons,
        [filter_by],
        filter_by == DROM_RAPPROCHES,
        fields,
    )
    return mapshaper.split(
        polygons,
        os.path.join(work_dir, "split"),
        fields[filter_by],
        crs,
        simplification,
        f"{config.PROVIDER}:{config.SOURCE}",
    )


def process_consolidated(
    inputs_dir: str,
    work_dir: str,
    level_polygons: str,
    layout: str,
    simplification: float,
    crs: int,
) -> str:
    """
    Produce the consolidated GeoParquet of one job from the prepared inputs.
    Returns its path.
    """
    fields = _read_fields(inputs_dir)
    levels = filter_levels(level_polygons, layout)
    polygons = _prepare_polygons(
        inputs_dir,
        work_dir,
        level_polygons,
        levels,
        layout == DROM_RAPPROCHES,
        fields,
    )
    geojson = mapshaper.finalize(
        polygons,
        os.path.join(work_dir, "final.geojson"),
        crs,
        simplification,
        f"{config.PROVIDER}:{config.SOURCE}",
    )
    sort_fields = [fields[x] for x in sort_levels(level_polygons, levels)]
    return to_consolidated_parquet(
        geojson,
        os.path.join(work_dir, "consolidated.parquet"),
        sort_fields=sort_fields,
        filter_columns={x: fields[x] for x in levels},
    )


def _connect_reading_gdal() -> duckdb.DuckDBPyConnection:
    """
    DuckDB connection for ST_Read (GDAL), which must run single-threaded:
    see cartiflette.ign.gpkg_to_parquet.
    """
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false; SET threads = 1;")
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true;")
    return con


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def to_consolidated_parquet(
    geojson_path: str,
    parquet_path: str,
    sort_fields: list[str],
    filter_columns: dict[str, str],
) -> str:
    """
    Convert a GeoJSON file to a GeoParquet (1.1) meant to be read partially:

    - rows sorted by `sort_fields` (see `sort_levels`) and written in small
      row groups, so that the rows of a value fit in few row groups: the
      clients read the filter column, then only the row groups holding the
      requested values;
    - a `bbox` column declared as the GeoParquet "covering" of the geometry,
      to filter by extent;
    - `filter_columns` (level used to filter -> column, e.g.
      BASSIN_VIE -> BV2022) stored in the file metadata under the key
      "cartiflette:filter_columns", for the clients.
    """
    with _connect_reading_gdal() as con:
        con.execute(
            "CREATE TABLE polygons AS SELECT * EXCLUDE (OGC_FID, geom), "
            f"geom AS geometry FROM ST_Read('{geojson_path}')"
        )
        # GeoParquet metadata as written by DuckDB (for the CRS), completed
        # with the bbox covering
        probe = parquet_path + ".probe"
        con.execute(
            f"COPY (SELECT * FROM polygons LIMIT 1) TO '{probe}' (FORMAT parquet)"
        )
        geo = json.loads(
            con.execute(
                f"SELECT value FROM parquet_kv_metadata('{probe}') WHERE key = 'geo'"
            ).fetchone()[0]
        )
        os.remove(probe)
        geometry_types = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT ST_GeometryType(geometry)::VARCHAR FROM polygons"
            ).fetchall()
        ]
        geo["version"] = "1.1.0"
        geo["columns"]["geometry"].update(
            {
                "geometry_types": sorted(
                    {"MULTIPOLYGON": "MultiPolygon", "POLYGON": "Polygon"}.get(t, t)
                    for t in geometry_types
                ),
                "covering": {
                    "bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}
                },
            }
        )
        geo["columns"]["geometry"].pop("bbox", None)

        con.execute("RESET threads")
        con.execute(
            f"""
            COPY (
                SELECT
                    * EXCLUDE (geometry),
                    geometry,
                    struct_pack(
                        xmin := ST_XMin(geometry),
                        ymin := ST_YMin(geometry),
                        xmax := ST_XMax(geometry),
                        ymax := ST_YMax(geometry)
                    ) AS bbox
                FROM polygons
                ORDER BY {", ".join(sort_fields)}
            ) TO '{parquet_path}' (
                FORMAT parquet,
                COMPRESSION zstd,
                ROW_GROUP_SIZE {config.ROW_GROUP_SIZE},
                GEOPARQUET_VERSION NONE,
                KV_METADATA {{
                    geo: {_sql_string(json.dumps(geo))},
                    "cartiflette:filter_columns": {_sql_string(json.dumps(filter_columns))}
                }}
            )
            """
        )
    return parquet_path


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
    """Run one GeoJSON job and upload its files to S3."""
    # Fail before any processing if the target is not allowed
    check_write_target(bucket, path_within_bucket)

    geojsons = process(
        inputs_dir, work_dir, level_polygons, filter_by, simplification, crs
    )
    uploaded = []
    for geojson in geojsons:
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
            value=os.path.basename(geojson).removesuffix(".geojson"),
            vectorfile_format="geojson",
            territory=config.TERRITORY,
            simplification=simplification,
        )
        uploaded.append(upload(geojson, remote, fs))

    shutil.rmtree(work_dir)
    return uploaded


def consolidate_and_upload(
    year: int,
    inputs_dir: str,
    work_dir: str,
    level_polygons: str,
    layout: str,
    simplification: float,
    crs: int,
    fs: s3fs.S3FileSystem,
    bucket: str = config.WRITE_BUCKET,
    path_within_bucket: str = config.WRITE_PATH,
) -> str:
    """Run one GeoParquet job and upload its file to S3."""
    check_write_target(bucket, path_within_bucket)

    parquet = process_consolidated(
        inputs_dir, work_dir, level_polygons, layout, simplification, crs
    )
    remote = create_path_consolidated(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=config.PROVIDER,
        dataset_family=config.DATASET_FAMILY,
        source=config.SOURCE,
        year=year,
        borders=level_polygons,
        crs=crs,
        layout=layout,
        simplification=simplification,
    )
    uploaded = upload(parquet, remote, fs)
    shutil.rmtree(work_dir)
    return uploaded
