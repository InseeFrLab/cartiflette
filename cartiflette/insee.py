"""
Insee "table d'appartenance géographique des communes" (TAGC): for each
commune, the codes of the supra-communal zonings (DEP, REG, EPCI, ZE2020,
UU2020, AAV2020, BV2022...).
"""

from __future__ import annotations

import os
import zipfile

import duckdb

from cartiflette.http import download_file, get_session

TAGC_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/7671844/"
    "table-appartenance-geo-communes-{year}.zip"
)
# The header of the TAGC spreadsheet is on its 6th row
TAGC_HEADER_CELL = "A6"


def fetch_tagc(year: int, dest_dir: str) -> str:
    """Download and unzip the TAGC of `year`; return the path to the xlsx."""
    os.makedirs(dest_dir, exist_ok=True)
    with get_session() as session:
        archive = download_file(
            TAGC_URL.format(year=year),
            os.path.join(dest_dir, f"tagc_{year}.zip"),
            session,
        )
    with zipfile.ZipFile(archive) as z:
        xlsx = [n for n in z.namelist() if n.endswith(".xlsx")]
        if len(xlsx) != 1:
            raise ValueError(f"Expected one xlsx in {archive}, found {xlsx}")
        z.extract(xlsx[0], dest_dir)
    os.remove(archive)
    return os.path.join(dest_dir, xlsx[0])


def tagc_to_parquet(xlsx_path: str, parquet_path: str) -> str:
    """
    Convert the TAGC spreadsheet to parquet (all columns as text). Empty
    columns at the right of the sheet (named C13, _1, _2...) are dropped.

    The excel extension of DuckDB is loaded in its own connection: loading it
    alongside the spatial extension corrupts geometry reading.
    """
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false; INSTALL excel; LOAD excel;")
        con.execute(
            "COPY (SELECT COLUMNS(c -> NOT regexp_matches(c, '^(C\\d+|_\\d+)$')) "
            f"FROM read_xlsx('{xlsx_path}', range='{TAGC_HEADER_CELL}:ZZ1000000', "
            "header=true, all_varchar=true, stop_at_empty=true)) "
            f"TO '{parquet_path}' (FORMAT parquet)"
        )
    return parquet_path
