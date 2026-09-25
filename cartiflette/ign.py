"""
Retrieval of ADMIN EXPRESS COG CARTO from the IGN Géoplateforme.

The download service exposes Atom feeds:
    {BASE}/resource/{RESOURCE}                 -> editions (sub-resources)
    {BASE}/resource/{RESOURCE}/{EDITION}       -> files of an edition
    {BASE}/download/{RESOURCE}/{EDITION}/{FILE}

Editions are named like
    ADMIN-EXPRESS-COG-CARTO_4-0__GEOPARQUET_WGS84G_FRA_2026-01-01
    ADMIN-EXPRESS-COG-CARTO_4-0__GPKG_WGS84G_FRA_2025-01-01
We use the "France entière" (FRA) editions in WGS84, which contain the DROM,
in GeoParquet when available and GPKG otherwise.
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
FORMAT_PREFERENCE = ("GEOPARQUET", "GPKG")

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


def gpkg_to_parquet(gpkg: str, layer: str, dest: str) -> str:
    """
    Convert a GPKG layer to GeoParquet.

    ST_Read (GDAL) is run single-threaded: with DuckDB 1.5.5, reading a GPKG
    with several threads randomly corrupts memory.
    """
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false; SET threads = 1;")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            f"COPY (SELECT * FROM ST_Read('{gpkg}', layer='{layer}')) "
            f"TO '{dest}' (FORMAT parquet)"
        )
    return dest


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

    gpkg = _extract_gpkg(archive, dest_dir)
    os.remove(archive)
    return {
        layer: "read_parquet('{}')".format(
            gpkg_to_parquet(gpkg, layer, os.path.join(dest_dir, f"{layer}.parquet"))
        )
        for layer in layers
    }
