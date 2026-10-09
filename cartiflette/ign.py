"""
Retrieval of ADMIN EXPRESS COG CARTO and Contours IRIS from the IGN
Géoplateforme. The catalogue, products, formats and territories are in
`sources.yaml` (see `sources`); the URLs of the files are read at runtime.

The download service exposes Atom feeds:
    {BASE}/resource/{RESOURCE}                 -> editions (sub-resources)
    {BASE}/resource/{RESOURCE}/{EDITION}       -> files of an edition
    {BASE}/download/{RESOURCE}/{EDITION}/{FILE}

Editions are named like
    ADMIN-EXPRESS-COG-CARTO_4-0__GEOPARQUET_WGS84G_FRA_2026-01-01
    ADMIN-EXPRESS-COG-CARTO_4-0__GPKG_WGS84G_FRA_2025-01-01
    ADMIN-EXPRESS-COG-CARTO_3-1__SHP_WGS84G_FRA_2022-04-15
We use the "France entière" (FRA) editions in WGS84, which contain the DROM,
in GeoParquet when available, then GPKG, then shapefile (editions 3-x, up to
2024). The layers are always returned with the field names of the edition 4-0.

Contours IRIS (CONTOURS-IRIS, not the generalized CONTOURS-IRIS-PE) shares
the commune boundaries of ADMIN EXPRESS COG CARTO of the same year. It exists
in France entière WGS84 from 2025 onwards, as GeoParquet, and one edition per
territory (GPKG, in the projection of the territory). The France entière
GeoParquet of 2025 was produced in June 2026, on the commune boundaries and
IRIS of 2026: for 2025, the editions per territory of June 2025 are used.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import xml.etree.ElementTree as ET
from functools import cache
from pathlib import Path

import duckdb
import py7zr
import requests
import yaml

from cartiflette.http import download_file, get_session

logger = logging.getLogger(__name__)

# Catalogue, products and editions of the IGN sources
SOURCES_PATH = Path(__file__).with_name("sources.yaml")


@cache
def sources() -> dict:
    """
    Metadata of the IGN sources (catalogue, products, formats, fields of the
    former editions, territories of Contours IRIS), from `sources.yaml`.
    """
    with open(SOURCES_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


_ATOM = {"atom": "http://www.w3.org/2005/Atom"}
_EDITION = re.compile(
    r"^(?P<resource>.+)_(?P<version>\d+-\d+)__(?P<format>[A-Z]+)_"
    r"(?P<crs>[A-Z0-9]+)_(?P<zone>[A-Z]{3})_(?P<date>\d{4}-\d{2}-\d{2})$"
)


def _atom_entries(url: str, session: requests.Session) -> list[ET.Element]:
    """All <entry> of a (paginated) Atom feed."""
    entries, page = [], 1
    while True:
        r = session.get(url, params={"page": page, "limit": 50})
        r.raise_for_status()
        feed = ET.fromstring(r.content)
        entries += feed.findall("atom:entry", _ATOM)
        page_count = int(
            feed.get(
                "{https://data.geopf.fr/annexes/ressources/xsd/gpf_dl.xsd}pagecount", 1
            )
        )
        if page >= page_count:
            return entries
        page += 1


def parse_edition(name: str) -> dict | None:
    match = _EDITION.match(name)
    return match.groupdict() if match else None


def list_editions(session: requests.Session, resource: str | None = None) -> list[str]:
    """Editions of `resource` (by default ADMIN EXPRESS COG CARTO)."""
    resource = resource or sources()["admin_express"]["resource"]
    entries = _atom_entries(f"{sources()['catalogue']}/resource/{resource}", session)
    return [e.findtext("atom:title", namespaces=_ATOM) for e in entries]


def select_edition(
    editions: list[str],
    year: int,
    formats: tuple[str, ...] | None = None,
) -> str:
    """
    Pick the France entière WGS84 edition of `year` in one of `formats`,
    preferring the first ones (by default those of ADMIN EXPRESS COG CARTO,
    see `sources`). If several editions exist for the same year, the latest
    one is taken.
    """
    formats = tuple(formats or sources()["admin_express"]["formats"])
    candidates = [
        (name, parsed)
        for name, parsed in ((n, parse_edition(n)) for n in editions)
        if parsed
        and parsed["zone"] == "FRA"
        and parsed["crs"] == "WGS84G"
        and parsed["date"].startswith(str(year))
        and parsed["format"] in formats
    ]
    if not candidates:
        raise ValueError(
            f"No France entière WGS84 edition in {'/'.join(formats)} for {year}"
        )
    candidates.sort(key=lambda c: (-formats.index(c[1]["format"]), c[1]["date"]))
    return candidates[-1][0]


def list_files(
    edition: str, session: requests.Session, resource: str | None = None
) -> list[str]:
    """Download URLs of the files of an edition (of `resource`, see `list_editions`)."""
    resource = resource or sources()["admin_express"]["resource"]
    entries = _atom_entries(
        f"{sources()['catalogue']}/resource/{resource}/{edition}", session
    )
    return [e.find("atom:link", _ATOM).get("href") for e in entries]


def _extract_gpkg(archive: str, dest_dir: str) -> str:
    with py7zr.SevenZipFile(archive, mode="r") as z:
        targets = [n for n in z.getnames() if n.lower().endswith(".gpkg")]
        if len(targets) != 1:
            raise ValueError(f"Expected one .gpkg in {archive}, found {targets}")
        z.extract(path=dest_dir, targets=targets)
    return os.path.join(dest_dir, targets[0])


def _extract_shapefiles(archive: str, dest_dir: str, names: list[str]) -> dict:
    """Extract the shapefiles `names` (e.g. "COMMUNE"); return their .shp paths."""
    with py7zr.SevenZipFile(archive, mode="r") as z:
        members = {
            n: os.path.basename(n).rsplit(".", 1)[0]
            for n in z.getnames()
            if os.path.basename(n).rsplit(".", 1)[0] in names
        }
        z.extract(path=dest_dir, targets=list(members))
    shp = {
        base: os.path.join(dest_dir, member)
        for member, base in members.items()
        if member.lower().endswith(".shp")
    }
    missing = set(names) - set(shp)
    if missing:
        raise ValueError(f"Shapefiles {missing} not found in {archive}")
    return shp


def _st_read_to_parquet(select: str, dest: str) -> str:
    """
    Write `select` (a query on ST_Read) to parquet.

    ST_Read (GDAL) is run single-threaded: with DuckDB 1.5.5, reading with
    several threads randomly corrupts memory.
    """
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false; SET threads = 1;")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"COPY ({select}) TO '{dest}' (FORMAT parquet)")
    return dest


def gpkg_to_parquet(gpkg: str, layer: str, dest: str) -> str:
    """Convert a GPKG layer to GeoParquet."""
    return _st_read_to_parquet(
        f"SELECT * FROM ST_Read('{gpkg}', layer='{layer}')", dest
    )


def shapefile_to_parquet(shp: str, fields: dict[str, str], dest: str) -> str:
    """
    Convert a shapefile of an edition 3-x to GeoParquet, keeping `fields`
    renamed ({former name: name in the edition 4-0}) and the geometry as
    `geometrie`, as in the edition 4-0.
    """
    columns = ", ".join(f'"{old}" AS {new}' for old, new in fields.items())
    return _st_read_to_parquet(
        f"SELECT {columns}, geom AS geometrie FROM ST_Read('{shp}')", dest
    )


def fetch_layers(year: int, layers: list[str], dest_dir: str) -> dict[str, str]:
    """
    Download the requested layers (e.g. "commune", "arrondissement_municipal")
    of the ADMIN EXPRESS COG CARTO edition of `year` into `dest_dir`, as
    GeoParquet files.

    Returns, for each layer, a DuckDB table expression reading it.
    """
    os.makedirs(dest_dir, exist_ok=True)
    with get_session() as session:
        edition = select_edition(list_editions(session), year)
        logger.info("Using IGN edition %s", edition)
        files = list_files(edition, session)
        edition_format = parse_edition(edition)["format"]

        if edition_format == "GEOPARQUET":
            by_layer = {os.path.basename(f).removesuffix(".parquet"): f for f in files}
            missing = set(layers) - set(by_layer)
            if missing:
                raise ValueError(f"Layers {missing} not found in {edition}")
            return {
                layer: "read_parquet('{}')".format(
                    download_file(
                        by_layer[layer],
                        os.path.join(dest_dir, f"{layer}.parquet"),
                        session,
                    )
                )
                for layer in layers
            }

        archives = [f for f in files if f.endswith(".7z")]
        if len(archives) != 1:
            # Archives above 4GB are split by the IGN, not expected here
            raise ValueError(f"Expected one .7z in {edition}, found {archives}")
        archive = download_file(
            archives[0], os.path.join(dest_dir, os.path.basename(archives[0])), session
        )

    if edition_format == "SHP":
        shapefile_layers = sources()["admin_express"]["shapefile_layers"]
        unknown = set(layers) - set(shapefile_layers)
        if unknown:
            raise ValueError(f"Layers {unknown} not handled for {edition}")
        shp = _extract_shapefiles(
            archive, dest_dir, [shapefile_layers[layer]["file"] for layer in layers]
        )
        os.remove(archive)
        return {
            layer: "read_parquet('{}')".format(
                shapefile_to_parquet(
                    shp[shapefile_layers[layer]["file"]],
                    shapefile_layers[layer]["fields"],
                    os.path.join(dest_dir, f"{layer}.parquet"),
                )
            )
            for layer in layers
        }

    gpkg = _extract_gpkg(archive, dest_dir)
    os.remove(archive)
    return {
        layer: "read_parquet('{}')".format(
            gpkg_to_parquet(gpkg, layer, os.path.join(dest_dir, f"{layer}.parquet"))
        )
        for layer in layers
    }


# GeoArrow "multipolygon" (list of polygons, of rings, of {x, y} points) as WKT
_GEOARROW_MULTIPOLYGON_WKT = """
    'MULTIPOLYGON(' || array_to_string(list_transform(geometrie, lambda polygon:
        '(' || array_to_string(list_transform(polygon, lambda ring:
            '(' || array_to_string(list_transform(ring, lambda point:
                point.x::VARCHAR || ' ' || point.y::VARCHAR
            ), ',') || ')'
        ), ',') || ')'
    ), ',') || ')'
"""


def iris_to_parquet(src: str, dest: str) -> str:
    """
    Copy the Contours IRIS GeoParquet `src` to `dest` with the geometry
    (`geometrie`) as a DuckDB geometry. The edition 2025 stores it in the
    GeoArrow "multipolygon" encoding, which DuckDB reads as nested lists:
    it is rebuilt from WKT (DuckDB prints doubles exactly).
    """
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false;")
        con.execute("INSTALL spatial; LOAD spatial;")
        types = dict(
            con.execute(
                f"SELECT column_name, column_type FROM (DESCRIBE '{src}')"
            ).fetchall()
        )
        geometry = (
            "geometrie"
            if types["geometrie"].startswith("GEOMETRY")
            else f"ST_GeomFromText({_GEOARROW_MULTIPOLYGON_WKT})"
        )
        con.execute(
            f"""
            COPY (
                SELECT * EXCLUDE (geometrie, geometrie_bbox), {geometry} AS geometrie
                FROM '{src}'
            ) TO '{dest}' (FORMAT parquet)
            """
        )
    return dest


def select_territory_editions(editions: list[str], year: int) -> dict[str, str]:
    """
    Contours IRIS GPKG editions of `year`, one per territory (see `sources`),
    the latest one if several.

    Raises
    ------
    ValueError
        If a territory has no edition for `year`.
    """
    iris = sources()["contours_iris"]
    found = {}
    for name in editions:
        parsed = parse_edition(name)
        if (
            parsed
            and parsed["format"] == "GPKG"
            and parsed["zone"] in iris["territories"]
            and parsed["date"].startswith(str(year))
        ):
            found[parsed["zone"]] = max(found.get(parsed["zone"], name), name)
    missing = [zone for zone in iris["territories"] if zone not in found]
    if missing:
        raise ValueError(f"No {iris['resource']} GPKG edition for {missing} in {year}")
    return {zone: found[zone] for zone in iris["territories"]}


def iris_gpkg_to_parquet(gpkgs: list[tuple[str, str]], dest: str) -> str:
    """
    Merge Contours IRIS GPKG files, each in the projection of its territory,
    into one GeoParquet in WGS84 (longitude, latitude), with the same fields
    as `iris_to_parquet`.

    Parameters
    ----------
    gpkgs : list of (str, str)
        Path of each GPKG and its CRS (e.g. "EPSG:2154").
    dest : str
        Output path.

    Returns
    -------
    str
        `dest`.
    """
    select = " UNION ALL ".join(
        f"SELECT * EXCLUDE (geometrie), "
        f"ST_Transform(geometrie, '{crs}', 'EPSG:4326') AS geometrie "
        f"FROM ST_Read('{path}')"
        for path, crs in gpkgs
    )
    # ST_Read (GDAL) single-threaded, see _st_read_to_parquet
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false; SET threads = 1;")
        con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true;")
        con.execute(f"COPY ({select}) TO '{dest}' (FORMAT parquet)")
    return dest


def _fetch_iris_by_territory(
    year: int, dest_dir: str, session: requests.Session
) -> str:
    """Contours IRIS of `year` from the editions per territory, as GeoParquet."""
    iris = sources()["contours_iris"]
    editions = select_territory_editions(list_editions(session, iris["resource"]), year)
    gpkgs = []
    for zone, edition in editions.items():
        logger.info("Using IGN edition %s", edition)
        archives = [
            f
            for f in list_files(edition, session, iris["resource"])
            if f.endswith(".7z")
        ]
        if len(archives) != 1:
            raise ValueError(f"Expected one .7z in {edition}, found {archives}")
        archive = download_file(
            archives[0], os.path.join(dest_dir, f"iris_{zone}.7z"), session
        )
        gpkg = _extract_gpkg(archive, os.path.join(dest_dir, f"iris_{zone}"))
        os.remove(archive)
        gpkgs.append((gpkg, iris["crs"][parse_edition(edition)["crs"]]))
    iris = iris_gpkg_to_parquet(gpkgs, os.path.join(dest_dir, "contours_iris.parquet"))
    for zone in editions:
        shutil.rmtree(os.path.join(dest_dir, f"iris_{zone}"))
    return iris


def fetch_iris(year: int, dest_dir: str) -> str | None:
    """
    Download the Contours IRIS of `year` (France entière, WGS84, GeoParquet)
    into `dest_dir`; for the years read by territory (`by_territory_years` in
    `sources`), the editions per territory, merged and reprojected to WGS84.

    Returns a DuckDB table expression reading it, with the fields of the IGN
    (code_insee, code_iris, nom_iris...) and the geometry as `geometrie`, or
    None if there is no such edition for `year` (before 2025).
    """
    iris = sources()["contours_iris"]
    os.makedirs(dest_dir, exist_ok=True)
    with get_session() as session:
        if year in iris["by_territory_years"]:
            iris = _fetch_iris_by_territory(year, dest_dir, session)
            return f"read_parquet('{iris}')"
        try:
            edition = select_edition(
                list_editions(session, iris["resource"]),
                year,
                formats=(iris["format"],),
            )
        except ValueError:
            logger.info("No %s edition for %s: no IRIS", iris["resource"], year)
            return None
        logger.info("Using IGN edition %s", edition)
        files = [
            f
            for f in list_files(edition, session, iris["resource"])
            if f.endswith(".parquet")
        ]
        if len(files) != 1:
            raise ValueError(f"Expected one .parquet in {edition}, found {files}")
        raw = download_file(
            files[0], os.path.join(dest_dir, "contours_iris_raw.parquet"), session
        )
    iris = iris_to_parquet(raw, os.path.join(dest_dir, "contours_iris.parquet"))
    os.remove(raw)
    return f"read_parquet('{iris}')"
