"""
Production pipeline: the functions called by the Argo workflow steps.

1. `prepare_year` -> inputs of a year (see cartiflette.prepare)
2. `consolidated_combinations` lists the (level, layout, simplification, crs)
   jobs;
3. `consolidate_and_upload` runs one of them: one GeoParquet per level and
   layout, filtered when read by the clients.

No GeoJSON is published: the API (api/) serves it from the GeoParquet.
"""

from __future__ import annotations

import json
import logging
import os
import shutil

import duckdb
import s3fs

from cartiflette import config, mapshaper
from cartiflette.paths import create_path_consolidated
from cartiflette.prepare import prepare_year  # noqa: F401 (pipeline step)
from cartiflette.s3 import check_write_target, upload

logger = logging.getLogger(__name__)

DROM_RAPPROCHES = "FRANCE_ENTIERE_DROM_RAPPROCHES"

# Levels of the polygons -> levels their GeoParquet can be filtered by
FILTERS = {
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
FILTERS["COMMUNE_ARRONDISSEMENT"] = FILTERS["COMMUNE"]
FILTERS["IRIS"] = ["COMMUNE", "COMMUNE_ARRONDISSEMENT", *FILTERS["COMMUNE"]]
# Levels read as is from the prepared inputs ({level}.geojson); the others
# are dissolved from the communes
BASE_LEVELS = ("COMMUNE", "COMMUNE_ARRONDISSEMENT", "IRIS")

# Percentage of points removed: complete contours, then lighter and lighter
SIMPLIFICATIONS = [0, 50, 80]
CRS = [4326]


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
        for level in FILTERS
        if levels is None or level in levels
        for layout in config.LAYOUTS
        for simplification in simplifications
        for crs in crs_list
    ]


def base_level(level_polygons: str) -> str:
    return level_polygons if level_polygons in BASE_LEVELS else "COMMUNE"


def available_levels(inputs_dir: str) -> list[str]:
    """Levels whose input was prepared: no IRIS before 2025."""
    return [
        level
        for level in FILTERS
        if os.path.exists(os.path.join(inputs_dir, f"{base_level(level)}.geojson"))
    ]


def filter_levels(level_polygons: str, layout: str) -> list[str]:
    """Levels a consolidated file of `layout` can be filtered by."""
    if layout == DROM_RAPPROCHES:
        return [DROM_RAPPROCHES]
    return [x for x in FILTERS[level_polygons] if x != DROM_RAPPROCHES]


def sort_levels(level_polygons: str, levels: list[str]) -> list[str]:
    """
    Levels a consolidated file is sorted by.

    Nested administrative levels first (region, departement), then, for
    communes, the living area (the smallest zoning, made of neighbouring
    communes). The rows of a region, a departement or a zoning are then
    contiguous and fit in one or two row groups, which the clients read
    alone. Without administrative level (zonings), the territory comes
    first.

    IRIS are mostly filtered by commune, which has too many values per row
    group for a bloom filter: they are sorted by departement and code (which
    starts with the code of the commune), so that the rows of a commune are
    contiguous.
    """
    available = {level_polygons, *levels}
    if level_polygons == "IRIS":
        return [x for x in ("DEPARTEMENT",) if x in available] + ["IRIS"]
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
    base = base_level(level_polygons)
    current = os.path.join(inputs_dir, f"{base}.geojson")

    if level_polygons != base:
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
      "cartiflette:filter_columns", for the clients;
    - one row per feature: rows with the same attributes are merged into
      one multipolygon. The Ile-de-France zoom of the DROM-rapproches layout
      (see `mapshaper.bring_drom_closer`) copies the features of Paris and
      its suburbs, which would otherwise come out twice when joined to data.
      The copies are disjoint and already simplified (by mapshaper, before),
      so the union only collects their parts.
    """
    with _connect_reading_gdal() as con:
        con.execute(
            f"""
            CREATE TABLE polygons AS
            SELECT
                * EXCLUDE (geometry),
                CASE WHEN count(*) = 1 THEN any_value(geometry)
                    ELSE ST_Union_Agg(geometry) END AS geometry
            FROM (
                SELECT * EXCLUDE (OGC_FID, geom), geom AS geometry
                FROM ST_Read('{geojson_path}')
            )
            GROUP BY ALL
            """
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
