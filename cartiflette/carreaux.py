"""
Gridded data of the Insee (données carroyées, dispositif Filosofi): squares
of 200 m and 1 km in metropolitan France, Martinique and La Réunion, with the
population, households, housing and income of their inhabitants (fields of
the Insee, kept as is: idcar_200m, ind, men, ind_snv...).

The Insee publishes one csv per territory, without geometry: the square is
built from the cell id (CRS3035RES200mN2029800E4252400: projection, size,
lower-left corner), in the projection of the territory, then reprojected to
WGS84.

Each cell is attached to one commune (or municipal arrondissement), the one
holding its centre, so that the cells of a commune, a departement or a
zoning tile it without duplicates. `lcog_geo` (Insee) keeps the list of the
communes the cell covers, in the geography of the data.
"""

from __future__ import annotations

import logging
import os
import re
import zipfile

import duckdb

from cartiflette import ign
from cartiflette.http import download_zip, get_session

logger = logging.getLogger(__name__)

# Levels of the grids -> field of the cell id
GRIDS = {"CARREAU_200M": "idcar_200m", "CARREAU_1KM": "idcar_1km"}

_CELL_ID = re.compile(
    r"^CRS(?P<crs>\d+)RES(?P<size>\d+)mN(?P<north>\d+)E(?P<east>\d+)$"
)
# Fields kept as text (ids, list of communes) and flags of imputed cells; the
# others are counts or sums
_TEXT_FIELDS = r"^(idcar_|lcog_)"
_FLAG_FIELDS = r"^i_est_"


def select_edition(year: int, editions: dict) -> int | None:
    """Latest edition whose data are not more recent than `year`."""
    candidates = [int(e) for e in editions if int(e) <= int(year)]
    return max(candidates) if candidates else None


def fetch_grid(year: int, level: str, dest_dir: str) -> tuple[int, list[str]] | None:
    """
    Download the grid `level` of the edition for `year` and unzip its csv
    (one per territory). Returns (edition, csv paths), None if no edition is
    old enough.
    """
    editions = ign.sources()["carreaux"]["editions"]
    edition = select_edition(year, editions)
    if edition is None:
        logger.info("No gridded data for %s", year)
        return None
    os.makedirs(dest_dir, exist_ok=True)
    with get_session() as session:
        archive = download_zip(
            editions[edition][level],
            os.path.join(dest_dir, f"{level}.zip"),
            session,
        )
    with zipfile.ZipFile(archive) as z:
        csvs = sorted(n for n in z.namelist() if n.endswith(".csv"))
        z.extractall(dest_dir, csvs)
    os.remove(archive)
    return edition, [os.path.join(dest_dir, n) for n in csvs]


def cell_crs(cell_id: str) -> str:
    """Projection of a cell, from its id."""
    match = _CELL_ID.match(cell_id)
    if not match:
        raise ValueError(f"Not an Insee cell id: {cell_id!r}")
    return f"EPSG:{match['crs']}"


def cells_query(csv: str, id_field: str, crs: str) -> str:
    """
    Cells of one csv (one territory, in projection `crs`): the Insee fields,
    the square (`geometry`) and its centre (`centre`), in WGS84.

    Read as text and cast: the type sniffed in a small file (an empty column)
    could differ from the other files.
    """
    return f"""
        SELECT
            * EXCLUDE (size, north, east),
            ST_Transform(
                ST_MakeEnvelope(east, north, east + size, north + size),
                '{crs}', 'EPSG:4326'
            ) AS geometry,
            ST_Transform(
                ST_Point(east + size / 2, north + size / 2), '{crs}', 'EPSG:4326'
            ) AS centre
        FROM (
            SELECT
                COLUMNS('{_TEXT_FIELDS}'),
                COLUMNS('{_FLAG_FIELDS}')::INTEGER,
                COLUMNS(c -> NOT regexp_matches(c, '{_TEXT_FIELDS}|{_FLAG_FIELDS}'))::DOUBLE,
                regexp_extract({id_field}, 'RES(\\d+)m', 1)::DOUBLE AS size,
                regexp_extract({id_field}, 'N(\\d+)E', 1)::DOUBLE AS north,
                regexp_extract({id_field}, 'E(\\d+)$', 1)::DOUBLE AS east
            FROM read_csv('{csv}', all_varchar = true)
        )
    """


def create_cells(
    con: duckdb.DuckDBPyConnection, csvs: list[str], id_field: str, table: str
) -> None:
    """Table `table` of the cells of all the territories (one csv each)."""
    queries = []
    for csv in csvs:
        first = con.execute(f"SELECT {id_field} FROM read_csv('{csv}') LIMIT 1")
        queries.append(cells_query(csv, id_field, cell_crs(first.fetchone()[0])))
    con.execute(f"CREATE TABLE {table} AS " + " UNION ALL BY NAME ".join(queries))


def attach_to_communes(
    con: duckdb.DuckDBPyConnection, cells: str, id_field: str, communes: str
) -> str:
    """
    Table `attached` (cell id, INSEE_COG): the commune or arrondissement of
    each cell of `cells`, from `communes` (INSEE_COG, geometry):

    1. the one holding the centre of the cell (the smallest code if the
       centre is on a boundary);
    2. for a centre at sea, the one covering the largest part of the cell;
    3. for a cell touching no commune (islets missing from the borders), the
       nearest one.
    """
    con.execute(
        f"""
        CREATE TABLE attached AS
        SELECT c.{id_field}, min(m.INSEE_COG) AS INSEE_COG
        FROM {cells} AS c JOIN {communes} AS m ON ST_Contains(m.geometry, c.centre)
        GROUP BY ALL
        """
    )
    con.execute(
        f"""
        INSERT INTO attached
        SELECT
            c.{id_field},
            arg_max(m.INSEE_COG, ST_Area(ST_Intersection(m.geometry, c.geometry)))
        FROM (SELECT * FROM {cells} ANTI JOIN attached USING ({id_field})) AS c
        JOIN {communes} AS m ON ST_Intersects(m.geometry, c.geometry)
        GROUP BY ALL
        """
    )
    con.execute(
        f"""
        INSERT INTO attached
        SELECT c.{id_field}, arg_min(m.INSEE_COG, ST_Distance(m.geometry, c.centre))
        FROM (SELECT * FROM {cells} ANTI JOIN attached USING ({id_field})) AS c
        CROSS JOIN {communes} AS m
        GROUP BY ALL
        """
    )
    counts = con.execute(
        f"SELECT (SELECT count(*) FROM {cells}), count(*), "
        f"count(DISTINCT {id_field}) FROM attached"
    ).fetchone()
    if len(set(counts)) != 1:
        raise ValueError(f"Cells, attachments, attached cells: {counts}")
    return "attached"


def grid_query(
    cells: str, id_field: str, attached: str, communes: str, source: str
) -> str:
    """
    Cells with the commune they are attached to (INSEE_COM, INSEE_COG,
    NOM_COM, AREA) and the source label, the square declared in EPSG:4326;
    the fields of the zonings are added by `prepare.enrich_query`, from
    INSEE_COM.
    """
    return f"""
        SELECT
            c.* EXCLUDE (geometry, centre),
            m.INSEE_COM,
            m.INSEE_COG,
            m.NOM AS NOM_COM,
            m.AREA,
            '{source}' AS SOURCE,
            -- Declared WGS84, written as PROJJSON in the GeoParquet metadata
            -- like the other files (OGC:CRS84, the default, would be omitted)
            ST_SetCRS(c.geometry, 'EPSG:4326') AS geometry
        FROM {cells} AS c
        JOIN {attached} AS a USING ({id_field})
        JOIN {communes} AS m ON a.INSEE_COG = m.INSEE_COG
    """
