"""
Test cartiflette client

The files are served from a local directory: `client.ENDPOINT_URL` is
replaced by it, DuckDB reading local paths and URLs the same way.
"""

import json

import duckdb
import geopandas as gpd
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


def test_create_path_consolidated():
    # Same test in tests/test_paths_and_s3.py of the pipeline
    assert create_path_consolidated(
        provider="IGN",
        dataset_family="ADMINEXPRESS",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        year=2025,
        borders="COMMUNE",
        crs=4326,
        layout="FRANCE_ENTIERE_DROM_RAPPROCHES",
        simplification=50.0,
    ) == (
        "projet-cartiflette/production/provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-CARTO-TERRITOIRE/year=2025/administrative_level=COMMUNE/"
        "crs=4326/layout=FRANCE_ENTIERE_DROM_RAPPROCHES/vectorfile_format=parquet/"
        "simplification=50/raw.parquet"
    )


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


@pytest.mark.parametrize(
    "fmt, year, expected",
    [
        ("auto", 2025, "parquet"),
        ("AUTO", "2026", "parquet"),
        ("auto", 2024, "geojson"),
        ("geojson", 2022, "geojson"),
        ("geoparquet", 2022, "parquet"),
    ],
)
def test_choose_format(fmt, year, expected):
    assert client.choose_format(fmt, year) == expected


def test_choose_format_parquet_exists():
    assert client.choose_format("auto", 2022, parquet_exists=True) == "parquet"
    assert client.choose_format("auto", 2025, parquet_exists=False) == "geojson"


def test_choose_format_prefers_geoparquet():
    with pytest.warns(UserWarning, match="GeoParquet is used instead"):
        assert client.choose_format("geojson", 2025) == "parquet"
    # 2022 republished with GeoParquet: its GeoJSON files can still be forced
    with pytest.warns(UserWarning, match="force=True"):
        assert (
            client.choose_format("geojson", 2022, force=True, parquet_exists=True)
            == "geojson"
        )


@pytest.mark.parametrize(
    "year, geojson_exists", [(2025, None), (2026, None), (2023, False)]
)
def test_choose_format_force_without_geojson(year, geojson_exists):
    # No GeoJSON file from 2025 onwards (or for a year republished by the
    # current pipeline): force=True is ignored
    with pytest.warns(UserWarning, match="no GeoJSON file.*force=True is ignored"):
        assert (
            client.choose_format(
                "geojson",
                year,
                force=True,
                parquet_exists=True,
                geojson_exists=geojson_exists,
            )
            == "parquet"
        )


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


DEPARTEMENTS = gpd.GeoDataFrame(
    {
        "INSEE_DEP": ["75", "92", "971", "01"],
        "INSEE_REG": ["11", "11", "01", "84"],
        "AREA": ["metropole", "metropole", "guadeloupe", "metropole"],
        "PAYS": ["France"] * 4,
        "POPULATION": [2, 1, 1, 1],
    },
    geometry=[Point(2.3, 48.8), Point(2.2, 48.8), Point(-61.5, 16.2), Point(5, 46)],
    crs=4326,
)


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Local directory standing for the S3 storage."""
    monkeypatch.setattr(client, "ENDPOINT_URL", str(tmp_path))
    return tmp_path


def _write_geojson(storage, region, year=2022, region_in_path=None):
    """GeoJSON of the departements of a region (legacy layout: REGION=1)."""
    path = storage / create_path_bucket(
        vectorfile_format="geojson",
        **{**PATH_KWARGS, "year": year, "value": region_in_path or region},
    )
    path.parent.mkdir(parents=True)
    DEPARTEMENTS[DEPARTEMENTS["INSEE_REG"] == region].to_file(path, driver="GeoJSON")


def _write_consolidated(storage, year, simplification=50):
    """Consolidated GeoParquet of departements, one row group per row."""
    path = storage / create_path_consolidated(
        layout="FRANCE_ENTIERE",
        **{
            **{
                k: v
                for k, v in PATH_KWARGS.items()
                if k not in ("value", "filter_by", "territory")
            },
            "year": year,
            "simplification": simplification,
        },
    )
    path.parent.mkdir(parents=True)
    DEPARTEMENTS.to_parquet(path, write_covering_bbox=True, row_group_size=1)
    table = pq.read_table(path)
    filters = {"REGION": "INSEE_REG", "TERRITOIRE": "AREA", "FRANCE_ENTIERE": "PAYS"}
    table = table.replace_schema_metadata(
        {**table.schema.metadata, client.FILTER_COLUMNS_KEY: json.dumps(filters)}
    )
    pq.write_table(table, path, row_group_size=1)
    return path


@pytest.fixture
def consolidated(storage):
    return _write_consolidated(storage, 2025)


def _download(**kwargs):
    return client.carti_download(
        **{
            "borders": "DEPARTEMENT",
            "filter_by": "REGION",
            "simplification": 50,
            **kwargs,
        }
    )


def test_geojson_concatenates_values(storage):
    _write_geojson(storage, "11")
    _write_geojson(storage, "84")
    gdf = _download(values=["11", "84"], year=2022)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert sorted(gdf["INSEE_DEP"]) == ["01", "75", "92"]
    assert gdf.crs.to_epsg() == 4326


def test_geojson_region_code_falls_back_to_unpadded(storage):
    # Files published before 2025 are stored under REGION=1, not REGION=01
    _write_geojson(storage, "01", region_in_path="1")
    for value in (1, "1", "01"):
        assert list(_download(values=value, year=2022)["INSEE_DEP"]) == ["971"]


def test_geojson_low_memory(storage):
    # maximum_object_size follows the file sizes: with a fixed 2 GB, DuckDB
    # allocated ~4 GB and failed in a 2 GiB container (API pods)
    _write_geojson(storage, "11")
    con = duckdb.connect()
    con.execute("SET memory_limit = '64MB'")
    gdf = _download(values="11", year=2022, con=con)
    assert sorted(gdf["INSEE_DEP"]) == ["75", "92"]


def test_geojson_missing_file_raises(storage):
    with pytest.raises((OSError, duckdb.IOException)):
        _download(values="11", year=2022)


@pytest.mark.usefixtures("consolidated")
@pytest.mark.parametrize(
    "filter_by, values, expected",
    [
        ("REGION", ["11"], ["75", "92"]),
        ("REGION", [1, "84"], ["01", "971"]),
        ("TERRITOIRE", "guadeloupe", ["971"]),
        ("FRANCE_ENTIERE", "France", ["01", "75", "92", "971"]),
    ],
)
def test_parquet(filter_by, values, expected):
    # year 2025: "auto" reads the GeoParquet
    gdf = _download(values=values, filter_by=filter_by, year=2025)
    assert sorted(gdf["INSEE_DEP"]) == expected
    assert "bbox" not in gdf.columns
    assert gdf.crs.to_epsg() == 4326


@pytest.mark.usefixtures("consolidated")
def test_geojson_requested_reads_geoparquet(storage):
    # No GeoJSON file for 2025 here: the GeoParquet is read
    with pytest.warns(UserWarning, match="GeoParquet is used instead"):
        gdf = _download(values="11", year=2025, vectorfile_format="geojson")
    assert sorted(gdf["INSEE_DEP"]) == ["75", "92"]


def test_geojson_forced(storage, readers):
    # 2022: GeoJSON files of the former pipeline, and a GeoParquet produced
    # afterwards
    _write_geojson(storage, "11")
    _write_consolidated(storage, 2022)
    with pytest.warns(UserWarning, match="Reading GeoJSON files as requested"):
        gdf = _download(values="11", year=2022, vectorfile_format="geojson", force=True)
    assert sorted(gdf["INSEE_DEP"]) == ["75", "92"]
    assert readers == ["read_geojson"]


@pytest.mark.parametrize("year", [2025, 2023])
def test_geojson_forced_without_geojson_files(storage, readers, year):
    # From 2025, or a year republished by the current pipeline: GeoParquet only
    _write_consolidated(storage, year)
    with pytest.warns(UserWarning, match="force=True is ignored"):
        gdf = _download(values="11", year=year, vectorfile_format="geojson", force=True)
    assert sorted(gdf["INSEE_DEP"]) == ["75", "92"]
    assert readers == ["read_parquet"]


@pytest.mark.parametrize(
    "year, file_exists, expected",
    [(2025, True, False), (2022, True, True), (2022, False, False)],
)
def test_geojson_available(storage, year, file_exists, expected):
    if file_exists:
        _write_geojson(storage, "11", year=year)
    kwargs = {k: v for k, v in PATH_KWARGS.items() if k not in ("value", "filter_by")}
    assert (
        client.geojson_available(["11"], "REGION", **{**kwargs, "year": year})
        is expected
    )


@pytest.mark.usefixtures("consolidated")
def test_engine_duckdb_returns_a_relation():
    con = duckdb.connect()
    rel = _download(values="11", year=2025, engine="duckdb", con=con)
    assert isinstance(rel, duckdb.DuckDBPyRelation)
    assert rel.aggregate("sum(POPULATION)").fetchone()[0] == 3
    assert "GEOMETRY" in str(rel.select("geometry").types[0])


@pytest.mark.usefixtures("consolidated")
def test_engine_duckdb_relation_is_lazy():
    # Not read yet: a later filter is applied while reading the file
    rel = _download(values="11", year=2025, engine="duckdb", where="POPULATION > 1")
    plan = rel.explain()
    assert "COLUMN_DATA_SCAN" not in plan
    assert "POPULATION>1" in plan.replace(" ", "")


def test_engine_duckdb_geojson(storage):
    _write_geojson(storage, "11")
    rel = _download(values="11", year=2022, engine="duckdb")
    assert rel.aggregate("count(*)").fetchone()[0] == 2


def test_unknown_engine():
    with pytest.raises(ValueError, match="engine"):
        _download(values="11", year=2025, engine="polars")


# Paris (2.3, 48.8), Hauts-de-Seine (2.2, 48.8), Guadeloupe, Ain (5, 46)
WHERE_CASES = [
    ("POPULATION > 1", ["75"]),
    ("INSEE_DEP IN ('92', '01') AND POPULATION = 1", ["01", "92"]),
    # Bounding box around Paris
    ("ST_Intersects(geometry, ST_MakeEnvelope(2.25, 48.7, 2.4, 48.9))", ["75"]),
    # Within 400 km of Paris: Ain is ~390 km away (~440 km if the coordinates
    # were read as latitude, longitude)
    (
        "ST_Distance_Sphere(ST_Centroid(geometry), ST_Point(2.35, 48.85)) < 400000",
        ["01", "75", "92"],
    ),
    (
        "ST_DWithin(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2154'), "
        "ST_Transform(ST_Point(2.35, 48.85), 'EPSG:4326', 'EPSG:2154'), 400000)",
        ["01", "75", "92"],
    ),
]


@pytest.mark.usefixtures("consolidated")
@pytest.mark.parametrize("where, expected", WHERE_CASES)
def test_parquet_where(where, expected):
    gdf = _download(values="France", filter_by="FRANCE_ENTIERE", year=2025, where=where)
    assert sorted(gdf["INSEE_DEP"]) == expected


@pytest.mark.parametrize("where, expected", WHERE_CASES)
def test_geojson_where(storage, where, expected):
    for region in ("11", "84", "01"):
        _write_geojson(storage, region, region_in_path="1" if region == "01" else None)
    gdf = _download(values=["11", "84", "01"], year=2022, where=where)
    assert sorted(gdf["INSEE_DEP"]) == expected


@pytest.mark.usefixtures("consolidated")
def test_where_engine_duckdb_and_json():
    rel = _download(values="11", year=2025, engine="duckdb", where="POPULATION > 1")
    assert rel.aggregate("count(*)").fetchone()[0] == 1
    geojson = json.loads(
        _download(values="11", year=2025, return_as_json=True, where="POPULATION > 1")
    )
    assert [f["properties"]["INSEE_DEP"] for f in geojson["features"]] == ["75"]


@pytest.mark.usefixtures("consolidated")
@pytest.mark.parametrize(
    "where", ["POPULATION >", "UNKNOWN_COLUMN > 1", "not_a_function(geometry)"]
)
def test_where_invalid(where):
    with pytest.raises(ValueError, match="Invalid where expression"):
        _download(values="11", year=2025, where=where)


@pytest.mark.usefixtures("consolidated")
def test_parquet_unknown_filter():
    with pytest.raises(ValueError, match="cannot be filtered by DEPARTEMENT"):
        _download(values="75", filter_by="DEPARTEMENT", year=2025)


@pytest.mark.usefixtures("consolidated")
def test_parquet_missing_value():
    with pytest.raises(ValueError, match="No REGION 99"):
        _download(values=["11", "99"], year=2025)


@pytest.fixture
def readers(monkeypatch):
    """Names of the reading functions called."""
    calls = []
    for name in ("read_parquet", "read_geojson"):
        function = getattr(client, name)
        monkeypatch.setattr(
            client,
            name,
            lambda *a, _f=function, _n=name, **k: calls.append(_n) or _f(*a, **k),
        )
    return calls


def test_parquet_produced_for_an_earlier_year(storage, readers):
    # GeoParquet produced afterwards for 2022: read instead of the GeoJSON
    _write_geojson(storage, "11")
    _write_consolidated(storage, 2022)
    with pytest.warns(UserWarning, match="GeoParquet is used instead"):
        gdf = _download(values="11", year=2022, vectorfile_format="geojson")
    assert sorted(gdf["INSEE_DEP"]) == ["75", "92"]
    _download(values="11", year=2022)
    assert readers == ["read_parquet", "read_parquet"]


def test_geojson_when_no_parquet_for_an_earlier_year(storage, readers):
    _write_geojson(storage, "11")
    assert sorted(_download(values="11", year=2022)["INSEE_DEP"]) == ["75", "92"]
    assert readers == ["read_geojson"]


@pytest.mark.parametrize(
    "year, file_exists, expected",
    [(2025, False, True), (2022, True, True), (2022, False, False)],
)
def test_parquet_available(storage, year, file_exists, expected):
    if file_exists:
        _write_consolidated(storage, year)
    kwargs = {k: v for k, v in PATH_KWARGS.items() if k not in ("value", "territory")}
    assert client.parquet_available(**{**kwargs, "year": year}) is expected


@pytest.mark.usefixtures("consolidated")
def test_parquet_missing_file():
    with pytest.raises(OSError, match="available from 2025"):
        _download(values="11", year=2022, vectorfile_format="parquet")


@pytest.mark.network
@pytest.mark.parametrize("value", [1, "1", "01"])
def test_production_drom_region(value):
    # Read-only: 2022 files are GeoJSON, stored under REGION=1
    gdf = client.carti_download(
        values=value,
        borders="DEPARTEMENT",
        filter_by="REGION",
        year=2022,
        simplification=50,
    )
    assert list(gdf["INSEE_DEP"]) == ["971"]


@pytest.mark.network
def test_production():
    # Read-only access to a file published in production
    gdf = client.carti_download(
        values=["11"],
        borders="DEPARTEMENT",
        vectorfile_format="geojson",
        simplification=50,
        filter_by="REGION",
        year=2022,
    )
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 8


def test_default_simplification_is_80(storage):
    # Without simplification, the lightest GeoParquet (80 % of points removed)
    _write_consolidated(storage, 2025, simplification=80)
    gdf = client.carti_download(
        values="11", borders="DEPARTEMENT", filter_by="REGION", year=2025
    )
    assert sorted(gdf["INSEE_DEP"]) == ["75", "92"]


def test_default_simplification_geojson_is_50(storage, readers):
    # The GeoJSON files of 2022 only exist with 0 and 50: 50 by default
    _write_geojson(storage, "11")
    gdf = client.carti_download(
        values="11", borders="DEPARTEMENT", filter_by="REGION", year=2022
    )
    assert sorted(gdf["INSEE_DEP"]) == ["75", "92"]
    assert readers == ["read_geojson"]
