# -*- coding: utf-8 -*-

import pytest

from cartiflette.utils import create_path_bucket


@pytest.mark.parametrize(
    "config, expected_path",
    [
        (
            {"bucket": "my_bucket"},
            (
                "my_bucket/PATH_WITHIN_BUCKET/2022/"
                "administrative_level=COMMUNE/2154/region=28/"
                "vectorfile_format=geojson/provider=IGN/"
                "source=EXPRESS-COG-TERRITOIRE/raw.geojson"
            ),
        ),
        (
            {"vectorfile_format": "shp"},
            (
                "BUCKET/PATH_WITHIN_BUCKET/2022/"
                "administrative_level=COMMUNE/2154/region=28/"
                "vectorfile_format=shp/provider=IGN/"
                "source=EXPRESS-COG-TERRITOIRE/"
            ),
        ),
        (
            {
                "borders": "DEPARTEMENT",
                "filter_by": "REGION",
                "year": "2023",
                "value": "42",
                "crs": 4326,
            },
            (
                "BUCKET/PATH_WITHIN_BUCKET/2023/"
                "administrative_level=DEPARTEMENT/4326/REGION=42/"
                "geojson/IGN/EXPRESS-COG-TERRITOIRE/raw.geojson"
            ),
        ),
        (
            {"path_within_bucket": "data", "vectorfile_format": "gpkg"},
            (
                "BUCKET/data/2022/"
                "administrative_level=COMMUNE/2154/region=28/"
                "gpkg/IGN/EXPRESS-COG-TERRITOIRE/raw.gpkg"
            ),
        ),
    ],
)
def test_create_path_bucket(config, expected_path):
    result = create_path_bucket(config)
    assert result == expected_path
