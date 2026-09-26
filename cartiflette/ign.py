"""
Retrieval of ADMIN EXPRESS COG CARTO from the IGN Géoplateforme.

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
"""

from __future__ import annotations

import logging
import os
import re
import xml.etree.ElementTree as ET

import duckdb
import py7zr
import requests

from cartiflette.http import download_file, get_session

logger = logging.getLogger(__name__)

BASE_URL = "https://data.geopf.fr/chunk/telechargement"
RESOURCE = "ADMIN-EXPRESS-COG-CARTO"
FORMAT_PREFERENCE = ("GEOPARQUET", "GPKG", "SHP")

# Editions 3-x (shapefile): one file per layer, with the former field names,
# renamed into the ones of the edition 4-0
SHAPEFILE_LAYERS = {
    "commune": (
        "COMMUNE",
        {
            "ID": "cleabs",
            "NOM": "nom_officiel",
            "INSEE_COM": "code_insee",
            "STATUT": "statut",
            "POPULATION": "population",
            "INSEE_DEP": "code_insee_du_departement",
        },
    ),
    "arrondissement_municipal": (
        "ARRONDISSEMENT_MUNICIPAL",
        {
            "ID": "cleabs",
            "NOM": "nom_officiel",
            "INSEE_ARM": "code_insee",
            "INSEE_COM": "code_insee_de_la_commune_de_rattach",
            "POPULATION": "population",
        },
    ),
    "departement": (
        "DEPARTEMENT",
        {"ID": "cleabs", "NOM": "nom_officiel", "INSEE_DEP": "code_insee"},
    ),
    "region": (
        "REGION",
        {"ID": "cleabs", "NOM": "nom_officiel", "INSEE_REG": "code_insee"},
    ),
}

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


def list_editions(session: requests.Session, resource: str = RESOURCE) -> list[str]:
    entries = _atom_entries(f"{BASE_URL}/resource/{resource}", session)
    return [e.findtext("atom:title", namespaces=_ATOM) for e in entries]


def select_edition(editions: list[str], year: int) -> str:
    """
    Pick the France entière WGS84 edition of `year`, preferring GeoParquet.
    If several editions exist for the same year, the latest one is taken.
    """
    candidates = [
        (name, parsed)
        for name, parsed in ((n, parse_edition(n)) for n in editions)
        if parsed
        and parsed["zone"] == "FRA"
        and parsed["crs"] == "WGS84G"
        and parsed["date"].startswith(str(year))
        and parsed["format"] in FORMAT_PREFERENCE
    ]
    if not candidates:
        raise ValueError(f"No France entière WGS84 edition of {RESOURCE} for {year}")
    candidates.sort(
        key=lambda c: (-FORMAT_PREFERENCE.index(c[1]["format"]), c[1]["date"])
    )
    return candidates[-1][0]


def list_files(
    edition: str, session: requests.Session, resource: str = RESOURCE
) -> list[str]:
    """Download URLs of the files of an edition."""
    entries = _atom_entries(f"{BASE_URL}/resource/{resource}/{edition}", session)
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
        unknown = set(layers) - set(SHAPEFILE_LAYERS)
        if unknown:
            raise ValueError(f"Layers {unknown} not handled for {edition}")
        shp = _extract_shapefiles(
            archive, dest_dir, [SHAPEFILE_LAYERS[layer][0] for layer in layers]
        )
        os.remove(archive)
        return {
            layer: "read_parquet('{}')".format(
                shapefile_to_parquet(
                    shp[SHAPEFILE_LAYERS[layer][0]],
                    SHAPEFILE_LAYERS[layer][1],
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
