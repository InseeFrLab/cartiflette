"""
Tests of the preparation queries on tiny tables mimicking the IGN
(ADMIN EXPRESS 4-0) and Insee (TAGC) sources.
"""

import pytest

from cartiflette import prepare

COMMUNE = """(SELECT * FROM (VALUES
    ('COMMUNE_1', 'Ville', '01001', 'Commune simple', 100, '01', ST_Point(5, 46)),
    ('COMMUNE_2', 'Paris', '75056', 'Capitale d''Etat', 2000, '75', ST_Point(2.3, 48.8)),
    ('COMMUNE_3', 'Basse-Terre', '97105', 'Préfecture', 50, '971', ST_Point(-61.7, 16)),
    ('COMMUNE_4', 'Saint-Pierre', '97502', 'Préfecture', 5, 'NR', ST_Point(-56, 46))
) t(cleabs, nom_officiel, code_insee, statut, population, code_insee_du_departement, geometrie))"""

ARRONDISSEMENT = """(SELECT * FROM (VALUES
    ('ARR_1', 'Paris 1er', '75101', '75056', 1000, ST_Point(2.34, 48.86)),
    ('ARR_2', 'Paris 2e', '75102', '75056', 1000, ST_Point(2.35, 48.87))
) t(cleabs, nom_officiel, code_insee, code_insee_de_la_commune_de_rattach, population, geometrie))"""

TAGC = """(SELECT * FROM (VALUES
    ('01001', 'Ville', '01', '84', '01004', '001'),
    ('75056', 'Paris', '75', '11', '75056', '001'),
    ('97105', 'Basse-Terre', '971', '01', '97105', '9A1')
) t(CODGEO, LIBGEO, DEP, REG, BV2022, AAV2020))"""

DEPARTEMENT = """(SELECT * FROM (VALUES ('01', 'Ain'), ('75', 'Paris'), ('971', 'Guadeloupe'))
    t(code_insee, nom_officiel))"""
REGION = """(SELECT * FROM (VALUES ('84', 'Auvergne-Rhône-Alpes'), ('11', 'Île-de-France'), ('01', 'Guadeloupe'))
    t(code_insee, nom_officiel))"""


@pytest.fixture(scope="module")
def con():
    con = prepare.connect()
    con.execute(f"CREATE TABLE communes AS {prepare.communes_query(COMMUNE)}")
    con.execute(f"CREATE TABLE metadata AS {prepare.metadata_query(TAGC, DEPARTEMENT, REGION)}")
    con.execute(
        "CREATE TABLE communes_arrondissement AS "
        + prepare.communes_arrondissement_query("communes", ARRONDISSEMENT)
    )
    yield con
    con.close()


def test_communes(con):
    rows = con.execute("SELECT INSEE_COM, AREA FROM communes ORDER BY 1").fetchall()
    # Saint-Pierre-et-Miquelon is not covered
    assert rows == [("01001", "metropole"), ("75056", "metropole"), ("97105", "guadeloupe")]


def test_communes_arrondissement(con):
    rows = con.execute(
        "SELECT INSEE_COM, INSEE_COG, STATUT FROM communes_arrondissement ORDER BY 2"
    ).fetchall()
    assert rows == [
        ("01001", "01001", "Commune simple"),
        ("75056", "75101", "Arrondissement municipal"),
        ("75056", "75102", "Arrondissement municipal"),
        ("97105", "97105", "Préfecture"),
    ]


def test_enrich(con):
    df = con.execute(prepare.enrich_query("communes", "metadata")).df()
    paris = df.set_index("INSEE_COM").loc["75056"]
    assert paris["INSEE_DEP"] == "75"
    assert paris["INSEE_REG"] == "11"
    assert paris["LIBELLE_DEPARTEMENT"] == "Paris"
    assert paris["LIBELLE_REGION"] == "Île-de-France"
    assert paris["BV2022"] == "75056"
    assert paris["PAYS"] == "France"
    assert "CODGEO" not in df.columns and "LIBGEO" not in df.columns
    # Codes keep their leading zeros
    assert df.set_index("INSEE_COM").loc["97105", "INSEE_REG"] == "01"


def test_write_geojson(con, tmp_path):
    path = prepare.write_geojson(
        con, prepare.enrich_query("communes", "metadata"), str(tmp_path / "c.geojson")
    )
    assert con.execute(f"SELECT count(*) FROM ST_Read('{path}')").fetchone()[0] == 3


def test_zoning_fields():
    columns = ["CODGEO", "DEP", "BV2012", "BV2022", "AAV2020", "UU2020", "ZE2020"]
    assert prepare.zoning_fields(columns) == {
        "BASSIN_VIE": "BV2022",
        "AIRE_ATTRACTION_VILLES": "AAV2020",
        "UNITE_URBAINE": "UU2020",
        "ZONE_EMPLOI": "ZE2020",
    }
    with pytest.raises(ValueError):
        prepare.zoning_fields(["CODGEO"])
