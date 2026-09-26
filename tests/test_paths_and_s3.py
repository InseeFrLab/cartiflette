from unittest import mock

import pytest

from cartiflette import config
from cartiflette.paths import create_path_bucket, create_path_consolidated
from cartiflette.s3 import check_write_target, upload


def test_create_path_bucket_production_layout():
    # Layout of the files read by the clients: must not change (same test in
    # python-package/cartiflette/tests/test_client.py)
    assert create_path_bucket(
        bucket="projet-cartiflette",
        path_within_bucket="production",
        provider="IGN",
        dataset_family="ADMINEXPRESS",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        year=2022,
        borders="DEPARTEMENT",
        crs=4326,
        filter_by="REGION",
        value="11",
        vectorfile_format="geojson",
        territory="metropole",
        simplification=50.0,
    ) == (
        "projet-cartiflette/production/provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-CARTO-TERRITOIRE/year=2022/"
        "administrative_level=DEPARTEMENT/crs=4326/REGION=11/"
        "vectorfile_format=geojson/territory=metropole/simplification=50/"
        "raw.geojson"
    )


def test_default_write_target_is_not_production():
    assert not (
        config.WRITE_BUCKET == config.PRODUCTION_BUCKET
        and config.WRITE_PATH.split("/")[0] == config.PRODUCTION_PATH
    )


@pytest.mark.parametrize("path", ["production", "production/", "/production/sub"])
def test_production_write_refused(path):
    with pytest.raises(PermissionError):
        check_write_target("projet-cartiflette", path)


@pytest.mark.parametrize(
    "bucket, path",
    [
        ("projet-cartiflette", "test"),
        ("projet-cartiflette", "production-test"),
        ("other", "production"),
    ],
)
def test_other_write_allowed(bucket, path):
    check_write_target(bucket, path)


def test_upload_to_production_does_not_touch_s3():
    fs = mock.Mock()
    with pytest.raises(PermissionError):
        upload("local.geojson", "projet-cartiflette/production/x/raw.geojson", fs)
    fs.put_file.assert_not_called()


def test_upload_to_test():
    fs = mock.Mock()
    upload("local.geojson", "projet-cartiflette/test/x/raw.geojson", fs)
    fs.put_file.assert_called_once_with(
        "local.geojson", "projet-cartiflette/test/x/raw.geojson"
    )


def test_create_path_consolidated_layout():
    # Same test in python-package/cartiflette/tests/test_client.py
    assert create_path_consolidated(
        bucket="projet-cartiflette",
        path_within_bucket="production",
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
