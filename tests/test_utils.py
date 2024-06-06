# -*- coding: utf-8 -*-

import pytest

from cartiflette.utils import create_path_bucket
from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET


@pytest.mark.parametrize(
    "config, expected_path",
    [
        # Teste qu'on substitue bien le bucket à la demande :
        (
            {"bucket": "my_bucket"},
            (
                f"my_bucket/{PATH_WITHIN_BUCKET}/"
                "provider=None/dataset_family=None/source=None/year=None/"
                "administrative_level=None/crs=2154/None=None/"
                "vectorfile_format=None/territory=None/simplification=0/"
                "raw.None"
            ),
        ),
        # Teste qu'on substitue bien le sous-dossier du bucjet à la demande :
        (
            {"path_within_bucket": "data", "vectorfile_format": "gpkg"},
            (
                f"{BUCKET}/data/"
                "projet-cartiflette/data/provider=None/dataset_family=None/"
                "source=None/year=None/administrative_level=None/crs=2154/"
                "None=None/vectorfile_format=gpkg/territory=None/"
                "simplification=0/raw.gpkg"
            ),
        ),
        # Teste que pour les shapefiles sans nom on crée bien un dossier :
        (
            {"vectorfile_format": "shp"},
            (
                f"{BUCKET}/{PATH_WITHIN_BUCKET}/"
                "provider=None/dataset_family=None/source=None/year=None/"
                "administrative_level=None/crs=2154/None=None/"
                "vectorfile_format=shp/territory=None/simplification=0/"
            ),
        ),
        # Teste que les arguments intermédiaires fonctionnent bien :
        (
            {
                "year": "2023",
                "borders": "DEPARTEMENT",
                "crs": 4326,
                "filter_by": "REGION",
                "value": "42",
            },
            (
                f"{BUCKET}/{PATH_WITHIN_BUCKET}/"
                "provider=None/dataset_family=None/source=None/"
                "year=2023/"
                "administrative_level=DEPARTEMENT/"
                "crs=4326/"
                "REGION=42/"
                "vectorfile_format=None/territory=None/simplification=0/"
                "raw.None"
            ),
        ),
    ],
)
def test_create_path_bucket(config, expected_path):
    result = create_path_bucket(config)
    assert result == expected_path
