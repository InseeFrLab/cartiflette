"""
Test the GeoJSON API

The files are served from a local directory: `client.ENDPOINT_URL` is
replaced by it, as in the tests of the client.
"""

import json

import geopandas as gpd
import pyarrow.parquet as pq
import pytest
from cartiflette import client
from cartiflette.utils import create_path_bucket, create_path_consolidated
from fastapi.testclient import TestClient
from shapely.geometry import Point

from cartiflette_api import app as api_module
from cartiflette_api import create_app

PATH_KWARGS = {
    "provider": "IGN",
    "dataset_family": "ADMINEXPRESS",
    "source": "EXPRESS-COG-CARTO-TERRITOIRE",
    "borders": "DEPARTEMENT",
    "crs": 4326,
    "simplification": 50,
}

DEPARTEMENTS = gpd.GeoDataFrame(
    {
        "INSEE_DEP": ["75", "92", "971", "01"],
        "INSEE_REG": ["11", "11", "01", "84"],
        "NOM": ["Paris", "Hauts-de-Seine", "Guadeloupe", "Ain"],
        "POPULATION": [1, 2, 3, 4],
    },
    geometry=[Point(2.3, 48.8), Point(2.2, 48.8), Point(-61.5, 16.2), Point(5, 46)],
    crs=4326,
)


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Local directory standing for the S3 storage, with a 2025 GeoParquet."""
    monkeypatch.setattr(client, "ENDPOINT_URL", str(tmp_path))
    path = tmp_path / create_path_consolidated(
        layout="FRANCE_ENTIERE", year=2025, **PATH_KWARGS
    )
    path.parent.mkdir(parents=True)
    DEPARTEMENTS.to_parquet(path, write_covering_bbox=True, row_group_size=1)
    table = pq.read_table(path)
    filters = {"REGION": "INSEE_REG", "FRANCE_ENTIERE": "PAYS"}
    table = table.replace_schema_metadata(
        {**table.schema.metadata, client.FILTER_COLUMNS_KEY: json.dumps(filters)}
    )
    pq.write_table(table, path, row_group_size=1)
    return tmp_path


@pytest.fixture
def api(storage):
    # The existence of the GeoParquet is cached, and the storage changes
    api_module.parquet_available.cache_clear()
    with TestClient(create_app()) as test_client:
        yield test_client


def _legacy_path(year, value):
    return create_path_bucket(
        year=year,
        filter_by="REGION",
        value=value,
        vectorfile_format="geojson",
        territory="metropole",
        **PATH_KWARGS,
    )


def _v1(api, follow_redirects=True, **params):
    return api.get(
        "/v1/geojson",
        params={
            "year": 2025,
            "borders": "DEPARTEMENT",
            "filter_by": "REGION",
            "simplification": 50,
            **params,
        },
        follow_redirects=follow_redirects,
    )


def test_health(api):
    assert api.get("/health").json() == {"status": "ok"}


@pytest.mark.parametrize(
    "values, expected",
    [(["11"], ["75", "92"]), (["11", "84"], ["01", "75", "92"]), ([1], ["971"])],
)
def test_v1(api, values, expected):
    response = _v1(api, values=values)
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/geo+json"
    collection = response.json()
    assert collection["type"] == "FeatureCollection"
    assert sorted(f["properties"]["INSEE_DEP"] for f in collection["features"]) == (
        expected
    )


def test_v1_same_as_client(api):
    collection = _v1(api, values=["11", "84"]).json()
    gdf = gpd.GeoDataFrame.from_features(collection, crs=4326)
    expected = client.carti_download(
        values=["11", "84"], year=2025, **{**PATH_KWARGS, "filter_by": "REGION"}
    )
    # Row order is not guaranteed, neither by the client nor by the API
    gdf = gdf[expected.columns].sort_values("INSEE_DEP", ignore_index=True)
    expected = expected.sort_values("INSEE_DEP", ignore_index=True)
    assert gdf.equals(expected)


def test_v1_gzip(api):
    response = _v1(api, values=["11", "84", "01"])
    assert response.headers["content-encoding"] == "gzip"
    assert len(response.json()["features"]) == 4


@pytest.mark.parametrize(
    "params, message",
    [
        ({"values": "99"}, "No REGION 99"),
        ({"values": "75", "filter_by": "DEPARTEMENT"}, "cannot be filtered"),
    ],
)
def test_v1_not_found(api, params, message):
    response = _v1(api, **params)
    assert response.status_code == 404
    assert message in response.json()["detail"]


@pytest.mark.parametrize("path", ["../production", "test/../..", "a//b", ""])
def test_v1_invalid_path_within_bucket(api, path):
    assert _v1(api, values="11", path_within_bucket=path).status_code == 400


def test_legacy_path(api):
    response = api.get("/" + _legacy_path(2025, "11"))
    assert response.status_code == 200
    features = response.json()["features"]
    assert sorted(f["properties"]["INSEE_DEP"] for f in features) == ["75", "92"]


def _write_geojson(storage, region, year=2022):
    """GeoJSON file of the departements of a region, legacy layout."""
    path = storage / _legacy_path(year, region)
    path.parent.mkdir(parents=True)
    DEPARTEMENTS[DEPARTEMENTS["INSEE_REG"] == region].to_file(path, driver="GeoJSON")


@pytest.mark.parametrize("value", ["11", "84"])
def test_v1_without_parquet_redirects(api, storage, value):
    response = _v1(api, values=value, year=2022, follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == f"{storage}/{_legacy_path(2022, value)}"


def test_v1_without_parquet_merges_several_values(api, storage):
    _write_geojson(storage, "11")
    _write_geojson(storage, "84")
    response = _v1(api, values=["11", "84"], year=2022)
    assert response.status_code == 200
    features = response.json()["features"]
    assert sorted(f["properties"]["INSEE_DEP"] for f in features) == [
        "01",
        "75",
        "92",
    ]


def test_v1_without_parquet_missing_file(api, storage):
    _write_geojson(storage, "11")
    response = _v1(api, values=["11", "84"], year=2022)
    assert response.status_code == 404


def test_legacy_path_without_parquet_redirects(api, storage):
    path = _legacy_path(2022, "11")
    response = api.get("/" + path, follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == f"{storage}/{path}"


@pytest.mark.parametrize(
    "path",
    [
        "favicon.ico",
        _legacy_path(2025, "11").replace("projet-cartiflette", "other-bucket"),
        _legacy_path(2025, "11").replace(".geojson", ".parquet"),
    ],
)
def test_legacy_path_unknown(api, path):
    assert api.get("/" + path).status_code == 404


@pytest.mark.parametrize(
    "borders, year, extension, expected",
    [
        ("DEPARTEMENT", 2026, "geojson", "DEP2026.geojson"),
        ("bassin_vie", "2023", "parquet", "BV2023.parquet"),
        ("COMMUNE_ARRONDISSEMENT", 2025, "geojson", "COMARM2025.geojson"),
        ("NOUVEAU_NIVEAU", 2026, "geojson", "NOUVEAU_NIVEAU2026.geojson"),
    ],
)
def test_file_name(borders, year, extension, expected):
    assert api_module.file_name(borders, year, extension) == expected


@pytest.mark.parametrize(
    "download, disposition", [(None, "inline"), ("1", "attachment")]
)
def test_v1_file_name(api, download, disposition):
    params = {"values": "11"} if download is None else {"values": "11", "download": 1}
    response = _v1(api, **params)
    assert response.status_code == 200
    assert response.headers["content-disposition"] == (
        f'{disposition}; filename="DEP2025.geojson"'
    )


def test_v1_without_parquet_download_is_not_redirected(api, storage):
    # A redirect cannot name the file: the GeoJSON file is read and sent
    _write_geojson(storage, "11")
    response = _v1(api, values="11", year=2022, download=1, follow_redirects=False)
    assert response.status_code == 200
    assert response.headers["content-disposition"] == (
        'attachment; filename="DEP2022.geojson"'
    )
    features = response.json()["features"]
    assert sorted(f["properties"]["INSEE_DEP"] for f in features) == ["75", "92"]


def _geoparquet(api, **params):
    return api.get(
        "/v1/geoparquet",
        params={
            "year": 2025,
            "borders": "DEPARTEMENT",
            "filter_by": "REGION",
            "simplification": 50,
            **params,
        },
    )


def test_geoparquet(api, tmp_path):
    response = _geoparquet(api, values=["11", "84"])
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.parquet"
    assert response.headers["content-disposition"] == (
        'attachment; filename="DEP2025.parquet"'
    )
    path = tmp_path / "telecharge.parquet"
    path.write_bytes(response.content)
    gdf = gpd.read_parquet(path)
    assert sorted(gdf["INSEE_DEP"]) == ["01", "75", "92"]
    assert gdf.crs.to_epsg() == 4326
    assert "bbox" not in gdf.columns


def test_geoparquet_temporary_file_deleted(api, monkeypatch, tmp_path):
    monkeypatch.setattr(api_module.tempfile, "tempdir", str(tmp_path))
    assert _geoparquet(api, values="11").status_code == 200
    assert list(tmp_path.glob("*.parquet")) == []


@pytest.mark.parametrize(
    "params, message",
    [
        ({"values": "99"}, "No REGION 99"),
        ({"values": "11", "year": 2022}, "No GeoParquet file"),
    ],
)
def test_geoparquet_not_found(api, params, message):
    response = _geoparquet(api, **params)
    assert response.status_code == 404
    assert message in response.json()["detail"]
