"""Gridded data of the Insee (cartiflette.carreaux), on tiny csv and tables."""

import json
import zipfile
from unittest import mock

import pytest

from cartiflette import carreaux, http, ign, pipeline, prepare

HEADER = "idcar_1km,i_est_1km,lcog_geo,ind,men,ind_snv"


def test_sources():
    editions = ign.sources()["carreaux"]["editions"]
    for urls in editions.values():
        assert set(urls) == set(carreaux.GRIDS)
        assert all(u.startswith("https://www.insee.fr/") for u in urls.values())
        assert all(u.endswith(".zip") for u in urls.values())


@pytest.mark.parametrize(
    "year, expected", [(2020, None), (2021, 2021), (2023, 2021), (2026, 2021)]
)
def test_select_edition(year, expected):
    assert carreaux.select_edition(year, {2021: {}}) == expected


def test_select_edition_latest_old_enough():
    assert carreaux.select_edition(2020, {2019: {}, 2021: {}}) == 2019


def test_cell_crs():
    assert carreaux.cell_crs("CRS3035RES200mN2029800E4252400") == "EPSG:3035"
    assert carreaux.cell_crs("CRS2975RES1000mN7634000E355000") == "EPSG:2975"
    with pytest.raises(ValueError):
        carreaux.cell_crs("2A041")


def test_create_cells(tmp_path):
    # Corsica (LAEA) and La Réunion (UTM 40S); in the small file, an empty
    # column is sniffed as text and must still be a number
    (tmp_path / "met.csv").write_text(
        f"{HEADER}\n"
        "CRS3035RES1000mN2029000E4252000,1,2A041,1.0,0.6,29914.7\n"
        "CRS3035RES1000mN2029000E4254000,0,2A04129212,2.5,1.1,59829.4\n"
    )
    (tmp_path / "reun.csv").write_text(
        f"{HEADER}\nCRS2975RES1000mN7634000E355000,0,97412,205.5,,3467867.8\n"
    )
    con = prepare.connect()
    carreaux.create_cells(
        con,
        [str(tmp_path / "met.csv"), str(tmp_path / "reun.csv")],
        "idcar_1km",
        "cells",
    )
    types = dict(
        con.execute("SELECT column_name, column_type FROM (DESCRIBE cells)").fetchall()
    )
    assert types["lcog_geo"] == "VARCHAR"
    assert types["i_est_1km"] == "INTEGER"
    assert types["men"] == types["ind"] == "DOUBLE"
    rows = con.execute(
        "SELECT idcar_1km, ST_X(centre), ST_Y(centre), ST_NPoints(geometry), "
        "ST_Contains(geometry, centre) FROM cells ORDER BY idcar_1km"
    ).fetchall()
    # Sorted by id: La Réunion (CRS2975) first
    reunion, corsica, _ = rows
    # Longitude, latitude
    assert 8.5 < corsica[1] < 9.6 and 41.3 < corsica[2] < 43.1
    assert 55.2 < reunion[1] < 55.9 and -21.5 < reunion[2] < -20.8
    # A square (closed ring) holding its centre
    assert all(r[3] == 5 and r[4] for r in rows)
    # 1 km side: about 0.009 degree of latitude
    height = con.execute(
        "SELECT ST_YMax(geometry) - ST_YMin(geometry) FROM cells LIMIT 1"
    ).fetchone()[0]
    assert height == pytest.approx(0.009, abs=0.0005)


COMMUNES = """(SELECT * FROM (VALUES
    ('A', 'A', 'Ville A', 'metropole', ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')),
    ('B', 'B', 'Ville B', 'metropole', ST_GeomFromText('POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))'))
) t(INSEE_COG, INSEE_COM, NOM, AREA, geometry))"""

# Cells (WGS84 squares and their centre): inside A; straddling, centre in B;
# centre at sea, mostly over B; out at sea, nearest to A
CELLS = """(SELECT id, ST_MakeEnvelope(x0, y0, x1, y1) AS geometry,
        ST_Point((x0 + x1) / 2, (y0 + y1) / 2) AS centre FROM (VALUES
    ('in_a', 0.1, 0.1, 0.3, 0.3),
    ('straddling', 0.9, 0.1, 1.3, 0.3),
    ('at_sea', 1.6, 0.9, 1.8, 1.3),
    ('islet', -0.5, 0.4, -0.3, 0.6)
) t(id, x0, y0, x1, y1))"""


def test_attach_to_communes():
    con = prepare.connect()
    con.execute(f"CREATE TABLE communes AS SELECT * FROM {COMMUNES}")
    con.execute(f"CREATE TABLE cells AS SELECT * FROM {CELLS}")
    attached = carreaux.attach_to_communes(con, "cells", "id", "communes")
    assert dict(con.execute(f"SELECT * FROM {attached}").fetchall()) == {
        "in_a": "A",
        "straddling": "B",
        "at_sea": "B",
        "islet": "A",
    }
    con.execute(
        "CREATE TABLE grid AS "
        + carreaux.grid_query(
            "cells", "id", attached, "communes", "INSEE:FILOSOFI-2021"
        )
    )
    assert con.execute("SELECT DISTINCT typeof(geometry) FROM grid").fetchall() == [
        ("GEOMETRY('EPSG:4326')",)
    ]
    columns = [c[0] for c in con.execute("DESCRIBE grid").fetchall()]
    assert columns == [
        "id",
        "INSEE_COM",
        "INSEE_COG",
        "NOM_COM",
        "AREA",
        "SOURCE",
        "geometry",
    ]


def test_download_zip_retries(tmp_path):
    dest = tmp_path / "grid.zip"
    good = tmp_path / "good.zip"
    with zipfile.ZipFile(good, "w") as z:
        z.writestr("carreaux.csv", "idcar_1km\n")
    attempts = []

    def fake_download(url, path, session):
        # Truncated first, complete then
        data = good.read_bytes()
        with open(path, "wb") as f:
            f.write(data if attempts else data[:20])
        attempts.append(url)
        return path

    with mock.patch.object(http, "download_file", fake_download):
        assert http.download_zip("https://x/grid.zip", str(dest), None) == str(dest)
    assert len(attempts) == 2

    with (
        mock.patch.object(http, "download_file", lambda u, p, s: open(p, "wb").close()),
        pytest.raises(OSError),
    ):
        http.download_zip("https://x/grid.zip", str(dest), None, attempts=2)
    assert not dest.exists()


def test_process_consolidated_grid(tmp_path):
    """A grid is written from its prepared parquet, without mapshaper."""
    con = prepare.connect()
    grid = f"""
        SELECT
            id AS idcar_1km, 1.0 AS ind, INSEE_COM, INSEE_COG, AREA,
            INSEE_COM AS INSEE_DEP, '84' AS INSEE_REG, '01004' AS BV2022,
            'France' AS PAYS, 'INSEE:FILOSOFI-2021' AS SOURCE, c.geometry
        FROM {CELLS} AS c
        JOIN (VALUES ('in_a', 'A'), ('straddling', 'B'), ('at_sea', 'B'), ('islet', 'A'))
            t(id, INSEE_COM) USING (id)
        JOIN (SELECT INSEE_COM, INSEE_COG, AREA FROM {COMMUNES}) USING (INSEE_COM)
    """
    prepare.write_parquet(con, grid, str(tmp_path / "CARREAU_1KM.parquet"))
    (tmp_path / "fields.json").write_text(json.dumps({"BASSIN_VIE": "BV2022"}))
    test_filters = {
        **pipeline.FILTERS,
        "CARREAU_1KM": [
            "COMMUNE",
            "COMMUNE_ARRONDISSEMENT",
            "BASSIN_VIE",
            "DEPARTEMENT",
        ],
    }

    with mock.patch.object(pipeline, "FILTERS", test_filters):
        parquet = pipeline.process_consolidated(
            str(tmp_path),
            str(tmp_path / "work"),
            "CARREAU_1KM",
            "FRANCE_ENTIERE",
            0,
            4326,
        )

    kv = dict(
        con.execute(
            f"SELECT decode(key), decode(value) FROM parquet_kv_metadata('{parquet}')"
        ).fetchall()
    )
    assert json.loads(kv["cartiflette:filter_columns"]) == {
        "COMMUNE": "INSEE_COM",
        "COMMUNE_ARRONDISSEMENT": "INSEE_COG",
        "BASSIN_VIE": "BV2022",
        "DEPARTEMENT": "INSEE_DEP",
    }
    geo = json.loads(kv["geo"])
    assert geo["columns"]["geometry"]["geometry_types"] == ["Polygon"]
    # One row per cell, sorted by departement then commune
    assert con.execute(f"SELECT idcar_1km, INSEE_COG FROM '{parquet}'").fetchall() == [
        ("in_a", "A"),
        ("islet", "A"),
        ("at_sea", "B"),
        ("straddling", "B"),
    ]
