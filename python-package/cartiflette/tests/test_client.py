#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test cartiflette client
"""

import geopandas as gpd
import pytest
from shapely.geometry import Point

from cartiflette import client
from cartiflette.utils import create_path_bucket, standardize_format


PATH_KWARGS = dict(
    provider="IGN",
    dataset_family="ADMINEXPRESS",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022,
    borders="DEPARTEMENT",
    crs=4326,
    filter_by="REGION",
    value="11",
    territory="metropole",
    simplification=50,
)


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
    [("geojson", "geojson"), ("GeoJSON", "geojson"), ("parquet", "parquet"), ("geoparquet", "parquet")],
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
        contents[f"https://minio.lab.sspcloud.fr/{path}"] = FakeResponse(local.read_bytes())

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
