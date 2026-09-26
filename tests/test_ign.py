from unittest import mock

import duckdb
import pytest

from cartiflette import ign, insee, prepare

EDITIONS = [
    "ADMIN-EXPRESS-COG-CARTO_3-2__SHP_WGS84G_FRA_2024-02-22",
    "ADMIN-EXPRESS-COG-CARTO_4-0__GPKG_LAMB93_FXX_2025-01-01",
    "ADMIN-EXPRESS-COG-CARTO_4-0__GPKG_WGS84G_FRA_2025-01-01",
    "ADMIN-EXPRESS-COG-CARTO_4-0__FLATGEOBUF_WGS84G_FRA_2026-01-01",
    "ADMIN-EXPRESS-COG-CARTO_4-0__GEOPARQUET_WGS84G_FRA_2026-01-01",
    "ADMIN-EXPRESS-COG-CARTO_4-0__GPKG_WGS84G_FRA_2026-01-01",
]


def test_parse_edition():
    assert ign.parse_edition(EDITIONS[4]) == {
        "resource": "ADMIN-EXPRESS-COG-CARTO",
        "version": "4-0",
        "format": "GEOPARQUET",
        "crs": "WGS84G",
        "zone": "FRA",
        "date": "2026-01-01",
    }
    assert ign.parse_edition("something else") is None


@pytest.mark.parametrize(
    "year, expected",
    [
        (2026, "ADMIN-EXPRESS-COG-CARTO_4-0__GEOPARQUET_WGS84G_FRA_2026-01-01"),
        (2025, "ADMIN-EXPRESS-COG-CARTO_4-0__GPKG_WGS84G_FRA_2025-01-01"),
        (2024, "ADMIN-EXPRESS-COG-CARTO_3-2__SHP_WGS84G_FRA_2024-02-22"),
    ],
)
def test_select_edition_prefers_geoparquet(year, expected):
    assert ign.select_edition(EDITIONS, year) == expected


def test_select_edition_missing_year():
    with pytest.raises(ValueError):
        ign.select_edition(EDITIONS, 2023)


def test_shapefile_to_parquet(tmp_path):
    """Layers of the editions 3-x get the field names of the edition 4-0."""
    shp = str(tmp_path / "COMMUNE.shp")
    with duckdb.connect() as con:
        con.execute("LOAD spatial")
        con.execute(
            "COPY (SELECT * FROM (VALUES "
            "('COMMUNE_1', 'Paris', 'PARIS', '75056', 'Capitale d''état', 2165423, "
            "'75', '11', ST_GeomFromText('POLYGON((2 48, 3 48, 3 49, 2 48))')), "
            "('COMMUNE_2', 'Les Abymes', 'LES ABYMES', '97101', 'Commune simple', "
            "53491, '971', '01', "
            "ST_GeomFromText('POLYGON((-61 16, -60 16, -60 17, -61 16))'))) "
            "AS t(ID, NOM, NOM_M, INSEE_COM, STATUT, POPULATION, INSEE_DEP, "
            "INSEE_REG, geom)) "
            f"TO '{shp}' WITH (FORMAT GDAL, DRIVER 'ESRI Shapefile')"
        )
    layer, fields = ign.SHAPEFILE_LAYERS["commune"]
    assert layer == "COMMUNE"
    parquet = ign.shapefile_to_parquet(shp, fields, str(tmp_path / "commune.parquet"))

    # The query written for the edition 4-0 runs on it unchanged
    communes = prepare.communes_query(f"read_parquet('{parquet}')")
    rows = (
        prepare.connect()
        .execute(
            "SELECT ID, NOM, INSEE_COM, STATUT, POPULATION, AREA, "
            f"ST_X(ST_Centroid(geometry)) FROM ({communes}) ORDER BY ID"
        )
        .fetchall()
    )
    assert [r[:6] for r in rows] == [
        ("COMMUNE_1", "Paris", "75056", "Capitale d'état", 2165423, "metropole"),
        ("COMMUNE_2", "Les Abymes", "97101", "Commune simple", 53491, "guadeloupe"),
    ]
    assert rows[1][6] < -60


@pytest.mark.parametrize(
    "year, filename",
    [
        (2022, "table-appartenance-geo-communes-22.zip"),
        (2023, "table-appartenance-geo-communes-23.zip"),
        ("2024", "table-appartenance-geo-communes-2024.zip"),
        (2026, "table-appartenance-geo-communes-2026.zip"),
    ],
)
def test_tagc_url(year, filename):
    assert insee.tagc_url(year).endswith("/7671844/" + filename)


FEED = """<?xml version='1.0' encoding='UTF-8'?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:gpf_dl="https://data.geopf.fr/annexes/ressources/xsd/gpf_dl.xsd"
      gpf_dl:page="{page}" gpf_dl:pagecount="2">
  <entry>
    <title>{name}</title>
    <link href="https://data.geopf.fr/x/{name}.parquet"/>
  </entry>
</feed>"""


def test_atom_pagination():
    responses = [
        mock.Mock(content=FEED.format(page=1, name="commune").encode()),
        mock.Mock(content=FEED.format(page=2, name="region").encode()),
    ]
    session = mock.Mock()
    session.get.side_effect = responses
    assert ign.list_files("EDITION", session) == [
        "https://data.geopf.fr/x/commune.parquet",
        "https://data.geopf.fr/x/region.parquet",
    ]
    assert session.get.call_count == 2
