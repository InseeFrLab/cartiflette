"""
Test cartiflette client
"""

import geopandas as gpd
import pytest
from cartiflette.utils import (
    create_path_bucket,
    standardize_format,
    value_candidates,
)
from shapely.geometry import Point

from cartiflette import client

PATH_KWARGS = {
    "provider": "IGN",
    "dataset_family": "ADMINEXPRESS",
    "source": "EXPRESS-COG-CARTO-TERRITOIRE",
    "year": 2022,
    "borders": "DEPARTEMENT",
    "crs": 4326,
    "filter_by": "REGION",
    "value": "11",
    "territory": "metropole",
    "simplification": 50,
}


def test_create_path_bucket():
    # Layout of the files published in production: must not change
    assert create_path_bucket(vectorfile_format="geojson", **PATH_KWARGS) == (
        "projet-cartiflette/production/provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-CARTO-TERRITOIRE/year=2022/"
        "administrative_level=DEPARTEMENT/crs=4326/REGION=11/"
        "vectorfile_format=geojson/territory=metropole/simplification=50/"
        "raw.geojson"
    )


def test_create_path_bucket_other_location():
    path = create_path_bucket(
        vectorfile_format="parquet",
        bucket="my-bucket",
        path_within_bucket="test",
        **{**PATH_KWARGS, "simplification": None},
    )
    assert path.startswith("my-bucket/test/provider=IGN/")
    assert path.endswith("/simplification=0/raw.parquet")


@pytest.mark.parametrize(
    "fmt, expected",
    [
        ("geojson", "geojson"),
        ("GeoJSON", "geojson"),
        ("parquet", "parquet"),
        ("geoparquet", "parquet"),
    ],
)
def test_standardize_format(fmt, expected):
    assert standardize_format(fmt) == expected


@pytest.mark.parametrize("fmt", ["topojson", "shp", "gpkg"])
def test_standardize_format_unsupported(fmt):
    with pytest.raises(ValueError):
        standardize_format(fmt)


class FakeResponse:
    def __init__(self, content, ok=True):
        self.content = content
        self.ok = ok
        self.status_code = 200 if ok else 404


class FakeSession:
    def __init__(self, contents):
        self.contents = contents
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        return self.contents.get(url, FakeResponse(b"", ok=False))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def _gdf(code):
    return gpd.GeoDataFrame({"INSEE_DEP": [code]}, geometry=[Point(2, 48)], crs=4326)


@pytest.mark.parametrize("fmt", ["geojson", "parquet"])
def test_carti_download_concatenates_values(monkeypatch, tmp_path, fmt):
    contents = {}
    for code in ("75", "92"):
        local = tmp_path / f"{code}.{fmt}"
        if fmt == "parquet":
            _gdf(code).to_parquet(local)
        else:
            _gdf(code).to_file(local, driver="GeoJSON")
        path = create_path_bucket(
            vectorfile_format=fmt, **{**PATH_KWARGS, "value": code}
        )
        contents[f"https://minio.lab.sspcloud.fr/{path}"] = FakeResponse(
            local.read_bytes()
        )

    session = FakeSession(contents)
    monkeypatch.setattr(client, "get_session", lambda: session)

    gdf = client.carti_download(
        values=["75", "92"],
        borders="DEPARTEMENT",
        filter_by="REGION",
        vectorfile_format=fmt,
        year=2022,
        simplification=50,
    )
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert sorted(gdf["INSEE_DEP"]) == ["75", "92"]


def test_carti_download_missing_file_raises(monkeypatch):
    monkeypatch.setattr(client, "get_session", lambda: FakeSession({}))
    with pytest.raises(IOError):
        client.carti_download(values="11", year=2022)


@pytest.mark.parametrize(
    "filter_by, value, expected",
    [
        ("REGION", 1, ["01", "1"]),
        ("REGION", "1", ["01", "1"]),
        ("REGION", "01", ["01", "1"]),
        ("region", "6", ["06", "6"]),
        ("REGION", "11", ["11"]),
        ("REGION", 84, ["84"]),
        ("DEPARTEMENT", "01", ["01"]),
        ("DEPARTEMENT", "2A", ["2A"]),
        ("FRANCE_ENTIERE", "France", ["France"]),
    ],
)
def test_value_candidates(filter_by, value, expected):
    assert value_candidates(filter_by, value) == expected


def test_region_code_falls_back_to_unpadded(monkeypatch, tmp_path):
    # Files published before 2025 are stored under REGION=1, not REGION=01
    local = tmp_path / "1.geojson"
    _gdf("971").to_file(local, driver="GeoJSON")
    path = create_path_bucket(
        vectorfile_format="geojson", **{**PATH_KWARGS, "value": "1"}
    )
    session = FakeSession(
        {f"https://minio.lab.sspcloud.fr/{path}": FakeResponse(local.read_bytes())}
    )
    monkeypatch.setattr(client, "get_session", lambda: session)

    gdf = client.carti_download(
        values="01", borders="DEPARTEMENT", year=2022, simplification=50
    )
    assert list(gdf["INSEE_DEP"]) == ["971"]
    assert [u.split("/REGION=")[1].split("/")[0] for u in session.urls] == ["01", "1"]


@pytest.mark.network
@pytest.mark.parametrize("value", [1, "1", "01"])
def test_carti_download_production_drom_region(value):
    # Read-only: 2022 files are stored under REGION=1
    gdf = client.carti_download(
        values=value,
        borders="DEPARTEMENT",
        filter_by="REGION",
        year=2022,
        simplification=50,
    )
    assert list(gdf["INSEE_DEP"]) == ["971"]


@pytest.mark.network
def test_carti_download_production():
    # Read-only access to a file published in production
    gdf = client.carti_download(
        values=["11"],
        crs=4326,
        borders="DEPARTEMENT",
        vectorfile_format="geojson",
        simplification=50,
        filter_by="REGION",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        year=2022,
    )
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 8
