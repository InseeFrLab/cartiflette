"""
Preparation of the pipeline inputs for a given year, from the IGN and Insee
sources. Writes into `local_dir`:

- COMMUNE.geojson: communes (WGS84) with the historical ADMIN EXPRESS field
  names (ID, NOM, INSEE_COM, STATUT, POPULATION), AREA (territory name) and,
  from the Insee TAGC, the codes of the supra-communal zonings (INSEE_DEP,
  INSEE_REG, EPCI, ZE2020, UU2020, AAV2020, BV2022...) with the labels of
  departements and regions and PAYS='France';
- COMMUNE_ARRONDISSEMENT.geojson: same, where Paris, Lyon and Marseille are
  replaced by their municipal arrondissements (INSEE_COG is the code of the
  arrondissement, INSEE_COM the one of the commune);
- IRIS.geojson (2025 onwards, see `ign.fetch_iris`): IRIS with the
  historical Contours IRIS field names (CODE_IRIS, NOM_IRIS, TYP_IRIS,
  NOM_COM), INSEE_COG (commune or arrondissement holding the IRIS),
  INSEE_COM, AREA and the same TAGC fields as the communes. No population;
- fields.json: name of the field holding each zoning (e.g. BASSIN_VIE ->
  BV2022), as the vintage of the zonings changes over time.

The territories covered are those historically covered by cartiflette:
metropole and the five DROM (Saint-Pierre-et-Miquelon is excluded).
"""

from __future__ import annotations

import json
import logging
import os
import re

import duckdb

from cartiflette import ign, insee

logger = logging.getLogger(__name__)

LAYERS = ["commune", "arrondissement_municipal", "departement", "region"]
CITIES_WITH_ARRONDISSEMENTS = ("75056", "69123", "13055")

# Fields of the zonings with a vintage in their name, found in the TAGC
ZONING_PREFIXES = {
    "BASSIN_VIE": "BV",
    "AIRE_ATTRACTION_VILLES": "AAV",
    "UNITE_URBAINE": "UU",
    "ZONE_EMPLOI": "ZE",
}

# Territory name (AREA field, used to split by TERRITOIRE)
AREA_FROM_DEPARTEMENT = """
    CASE code_insee_du_departement
        WHEN '971' THEN 'guadeloupe'
        WHEN '972' THEN 'martinique'
        WHEN '973' THEN 'guyane'
        WHEN '974' THEN 'reunion'
        WHEN '976' THEN 'mayotte'
        ELSE 'metropole'
    END
"""


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    con.execute("INSTALL spatial; LOAD spatial;")
    # Coordinates are always (longitude, latitude), whatever the CRS says
    con.execute("SET geometry_always_xy = true")
    return con


def write_geojson(con: duckdb.DuckDBPyConnection, query: str, path: str) -> str:
    """Write the result of `query` (WGS84, `geometry` column) as GeoJSON."""
    if os.path.exists(path):
        os.remove(path)
    con.execute(
        f"COPY ({query}) TO '{path}' WITH (FORMAT GDAL, DRIVER 'GeoJSON', "
        "LAYER_CREATION_OPTIONS 'COORDINATE_PRECISION=7')"
    )
    return path


def communes_query(commune: str) -> str:
    return f"""
        SELECT
            cleabs AS ID,
            nom_officiel AS NOM,
            code_insee AS INSEE_COM,
            statut AS STATUT,
            population AS POPULATION,
            {AREA_FROM_DEPARTEMENT} AS AREA,
            geometrie AS geometry
        FROM {commune}
        WHERE code_insee_du_departement <> 'NR'
    """


def communes_arrondissement_query(communes: str, arrondissement_municipal: str) -> str:
    return f"""
        SELECT * EXCLUDE (INSEE_COM), INSEE_COM, INSEE_COM AS INSEE_COG
        FROM {communes}
        WHERE INSEE_COM NOT IN {CITIES_WITH_ARRONDISSEMENTS}
        UNION ALL BY NAME
        SELECT
            cleabs AS ID,
            nom_officiel AS NOM,
            code_insee_de_la_commune_de_rattach AS INSEE_COM,
            code_insee AS INSEE_COG,
            'Arrondissement municipal' AS STATUT,
            population AS POPULATION,
            'metropole' AS AREA,
            geometrie AS geometry
        FROM {arrondissement_municipal}
    """


def iris_query(iris: str, communes_arrondissement: str) -> str:
    """
    IRIS with the fields of their commune. The IGN codes the IRIS of Paris,
    Lyon and Marseille by arrondissement, hence the join on INSEE_COG. It
    drops the IRIS of the territories not covered (Saint-Pierre-et-Miquelon,
    Saint-Barthélemy, Saint-Martin).
    """
    return f"""
        SELECT
            iris.cleabs AS ID,
            iris.code_iris AS CODE_IRIS,
            iris.nom_iris AS NOM_IRIS,
            iris.type_iris AS TYP_IRIS,
            iris.nom_commune AS NOM_COM,
            com.INSEE_COM,
            com.INSEE_COG,
            com.AREA,
            iris.geometrie AS geometry
        FROM {iris} AS iris
        JOIN {communes_arrondissement} AS com ON iris.code_insee = com.INSEE_COG
    """


def metadata_query(tagc: str, departement: str, region: str) -> str:
    """TAGC with the labels of departements and regions, keyed by CODGEO."""
    return f"""
        SELECT
            tagc.* EXCLUDE (LIBGEO, DEP, REG),
            tagc.DEP AS INSEE_DEP,
            tagc.REG AS INSEE_REG,
            dep.nom_officiel AS LIBELLE_DEPARTEMENT,
            reg.nom_officiel AS LIBELLE_REGION
        FROM {tagc} AS tagc
        LEFT JOIN {departement} AS dep ON tagc.DEP = dep.code_insee
        LEFT JOIN {region} AS reg ON tagc.REG = reg.code_insee
    """


def enrich_query(polygons: str, metadata: str) -> str:
    return f"""
        SELECT
            p.* EXCLUDE (geometry),
            m.* EXCLUDE (CODGEO),
            'France' AS PAYS,
            p.geometry
        FROM {polygons} AS p
        LEFT JOIN {metadata} AS m ON p.INSEE_COM = m.CODGEO
        ORDER BY p.INSEE_COM
    """


def check_iris_cover_communes(
    con: duckdb.DuckDBPyConnection, iris: str, communes_arrondissement: str
) -> None:
    """Every commune (or arrondissement) must hold at least one IRIS."""
    missing = [
        row[0]
        for row in con.execute(
            f"""
            SELECT INSEE_COG FROM {communes_arrondissement}
            WHERE INSEE_COG NOT IN (SELECT INSEE_COG FROM {iris})
            ORDER BY 1
            """
        ).fetchall()
    ]
    if missing:
        raise ValueError(f"No IRIS for the communes {missing}")


def zoning_fields(columns: list[str]) -> dict[str, str]:
    """Map each zoning with a vintage to its field, e.g. BASSIN_VIE -> BV2022."""
    fields = {}
    for zoning, prefix in ZONING_PREFIXES.items():
        matches = sorted(c for c in columns if re.fullmatch(rf"{prefix}\d{{4}}", c))
        if not matches:
            raise ValueError(f"No {prefix}YYYY field for {zoning} in {columns}")
        fields[zoning] = matches[-1]
    return fields


def prepare_year(year: int, local_dir: str) -> dict[str, str]:
    """
    Download the sources of `year` and write the pipeline inputs into
    `local_dir`. Returns the paths of the written files.
    """
    raw_dir = os.path.join(local_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    tables = ign.fetch_layers(year, LAYERS, raw_dir)
    tagc = "read_parquet('{}')".format(
        insee.tagc_to_parquet(
            insee.fetch_tagc(year, raw_dir), os.path.join(raw_dir, "tagc.parquet")
        )
    )

    con = connect()
    con.execute(f"CREATE TABLE communes AS {communes_query(tables['commune'])}")
    con.execute(
        "CREATE TABLE metadata AS "
        + metadata_query(tagc, tables["departement"], tables["region"])
    )
    con.execute(
        "CREATE TABLE communes_arrondissement AS "
        + communes_arrondissement_query("communes", tables["arrondissement_municipal"])
    )
    levels = [
        ("COMMUNE", "communes"),
        ("COMMUNE_ARRONDISSEMENT", "communes_arrondissement"),
    ]
    iris = ign.fetch_iris(year, raw_dir)
    if iris is not None:
        con.execute(
            f"CREATE TABLE iris AS {iris_query(iris, 'communes_arrondissement')}"
        )
        check_iris_cover_communes(con, "iris", "communes_arrondissement")
        levels.append(("IRIS", "iris"))

    outputs = {
        level: write_geojson(
            con,
            enrich_query(table, "metadata"),
            os.path.join(local_dir, f"{level}.geojson"),
        )
        for level, table in levels
    }

    columns = [c[0] for c in con.execute("DESCRIBE metadata").fetchall()]
    outputs["fields"] = os.path.join(local_dir, "fields.json")
    with open(outputs["fields"], "w") as f:
        json.dump(zoning_fields(columns), f)

    con.close()
    logger.info("Prepared inputs for %s: %s", year, outputs)
    return outputs
