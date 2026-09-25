from unittest import mock

import pytest

from cartiflette import ign

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
    ],
)
def test_select_edition_prefers_geoparquet(year, expected):
    assert ign.select_edition(EDITIONS, year) == expected


def test_select_edition_missing_year():
    # 2024 only exists as shapefile
    with pytest.raises(ValueError):
        ign.select_edition(EDITIONS, 2024)


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
