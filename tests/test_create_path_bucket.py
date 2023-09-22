import pytest
from cartiflette.utils import (
    create_path_bucket
)
# Import create_path_bucket function and VectorFileConfig here (if not already imported)

# Define some test cases with different configurations
@pytest.mark.parametrize(
    "config, expected_path",
    [
        ({"bucket": "my_bucket"}, 'my_bucket/diffusion/shapefiles-test1/year=2022/administrative_level=COMMUNE/crs=2154/region=28/vectorfile_format=geojson/provider=IGN/source=EXPRESS-COG-TERRITOIRE/raw.geojson'),
        ({"vectorfile_format": "shp"}, 'projet-cartiflette/diffusion/shapefiles-test1/year=2022/administrative_level=COMMUNE/crs=2154/region=28/vectorfile_format=shp/provider=IGN/source=EXPRESS-COG-TERRITOIRE/'),
        ({"borders": "DEPARTEMENT", "filter_by": "REGION", "year": "2023", "value": "42", "crs": 4326}, 'projet-cartiflette/diffusion/shapefiles-test1/year=2023/administrative_level=DEPARTEMENT/crs=4326/REGION=42/vectorfile_format=geojson/provider=IGN/source=EXPRESS-COG-TERRITOIRE/raw.geojson'),
        ({"path_within_bucket": "data", "vectorfile_format": "gpkg"}, 'projet-cartiflette/data/year=2022/administrative_level=COMMUNE/crs=2154/region=28/vectorfile_format=gpkg/provider=IGN/source=EXPRESS-COG-TERRITOIRE/raw.gpkg'),
    ],
)
def test_create_path_bucket(config, expected_path):
    result = create_path_bucket(config)
    assert result == expected_path

# Run the tests
if __name__ == "__main__":
    pytest.main()
