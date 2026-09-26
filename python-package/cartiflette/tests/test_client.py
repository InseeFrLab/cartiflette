"""
Test cartiflette client
"""

import json

import geopandas as gpd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from cartiflette.utils import (
    create_path_bucket,
    create_path_consolidated,
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


def test_carti_download_concatenates_values(monkeypatch, tmp_path):
    fmt = "geojson"
    contents = {}
    for code in ("75", "92"):
        local = tmp_path / f"{code}.{fmt}"
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


def test_create_path_consolidated():
    # Same test in tests/test_paths_and_s3.py of the pipeline
    assert create_path_consolidated(
        provider="IGN",
        dataset_family="ADMINEXPRESS",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        year=2025,
        borders="COMMUNE",
        crs=4326,
        geometry="FRANCE_ENTIERE_DROM_RAPPROCHES",
        simplification=50.0,
    ) == (
        "projet-cartiflette/production/provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-CARTO-TERRITOIRE/year=2025/administrative_level=COMMUNE/"
        "crs=4326/geometry=FRANCE_ENTIERE_DROM_RAPPROCHES/vectorfile_format=parquet/"
        "simplification=50/raw.parquet"
    )


CONSOLIDATED_KWARGS = {
    "provider": "IGN",
    "dataset_family": "ADMINEXPRESS",
    "source": "EXPRESS-COG-CARTO-TERRITOIRE",
    "year": 2025,
    "borders": "DEPARTEMENT",
    "crs": 4326,
    "simplification": 50,
}


@pytest.fixture
def consolidated_storage(tmp_path, monkeypatch):
    """Local storage holding a consolidated file of departements."""
    gdf = gpd.GeoDataFrame(
        {
            "INSEE_DEP": ["75", "92", "971", "01"],
            "INSEE_REG": ["11", "11", "01", "84"],
            "AREA": ["metropole", "metropole", "guadeloupe", "metropole"],
            "PAYS": ["France"] * 4,
        },
        geometry=[Point(2.3, 48.8), Point(2.2, 48.8), Point(-61.5, 16.2), Point(5, 46)],
        crs=4326,
    )
    gdf["bbox"] = [
        {"xmin": p.x, "ymin": p.y, "xmax": p.x, "ymax": p.y} for p in gdf.geometry
    ]
    path = tmp_path / create_path_consolidated(
        geometry="FRANCE_ENTIERE", **CONSOLIDATED_KWARGS
    )
    path.parent.mkdir(parents=True)
    # One row group per departement, to check that only the needed ones are read
    gdf.to_parquet(path, row_group_size=1)
    table = pq.read_table(path)
    filters = {"REGION": "INSEE_REG", "TERRITOIRE": "AREA", "FRANCE_ENTIERE": "PAYS"}
    table = table.replace_schema_metadata(
        {**table.schema.metadata, client.FILTER_COLUMNS_KEY: json.dumps(filters)}
    )
    pq.write_table(table, path, row_group_size=1)

    fs = pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem())
    monkeypatch.setattr(client, "get_s3_filesystem", lambda: fs)


@pytest.mark.parametrize(
    "filter_by, values, expected",
    [
        ("REGION", ["11"], ["75", "92"]),
        ("REGION", [1, "84"], ["01", "971"]),
        ("TERRITOIRE", "guadeloupe", ["971"]),
        ("FRANCE_ENTIERE", "France", ["01", "75", "92", "971"]),
    ],
)
@pytest.mark.usefixtures("consolidated_storage")
def test_carti_download_parquet(filter_by, values, expected):
    gdf = client.carti_download(
        values=values,
        borders="DEPARTEMENT",
        filter_by=filter_by,
        vectorfile_format="parquet",
        year=2025,
        simplification=50,
    )
    assert sorted(gdf["INSEE_DEP"]) == expected
    assert "bbox" not in gdf.columns
    assert gdf.crs.to_epsg() == 4326


def test_select_row_groups(tmp_path):
    table = pa.table({"INSEE_REG": ["01", "11", "11", "84"], "x": [1, 2, 3, 4]})
    pq.write_table(table, tmp_path / "t.parquet", row_group_size=1)
    parquet_file = pq.ParquetFile(tmp_path / "t.parquet")
    assert client.select_row_groups(parquet_file, "INSEE_REG", ["11"]) == [1, 2]
    assert client.select_row_groups(parquet_file, "INSEE_REG", ["01", "84"]) == [0, 3]
    assert client.select_row_groups(parquet_file, "INSEE_REG", ["99"]) == []


@pytest.mark.usefixtures("consolidated_storage")
def test_carti_download_parquet_unknown_filter():
    with pytest.raises(ValueError, match="cannot be filtered by DEPARTEMENT"):
        client.carti_download(
            values="75",
            borders="DEPARTEMENT",
            filter_by="DEPARTEMENT",
            vectorfile_format="parquet",
            year=2025,
            simplification=50,
        )


@pytest.mark.usefixtures("consolidated_storage")
def test_carti_download_parquet_missing_value():
    with pytest.raises(ValueError, match="No REGION 99"):
        client.carti_download(
            values=["11", "99"],
            borders="DEPARTEMENT",
            filter_by="REGION",
            vectorfile_format="parquet",
            year=2025,
            simplification=50,
        )


@pytest.mark.usefixtures("consolidated_storage")
def test_carti_download_parquet_missing_file():
    with pytest.raises(OSError, match="available from 2025"):
        client.carti_download(
            values="11",
            borders="DEPARTEMENT",
            filter_by="REGION",
            vectorfile_format="parquet",
            year=2022,
            simplification=50,
        )


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
