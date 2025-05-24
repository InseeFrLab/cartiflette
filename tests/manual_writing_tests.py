from cartiflette.config import BUCKET, FS
from cartiflette.utils import create_path_bucket
from cartiflette.pipeline.combine_adminexpress_france import (
    combine_adminexpress_territory,
)
from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata

from cartiflette.pipeline import (
    mapshaperize_split_from_s3,
    mapshaperize_merge_split_from_s3,
)

# DATA RETRIEVING STEP =========================

bucket = BUCKET
path_within_bucket = "test/test-bv"
year = 2022
fs = FS


# PART 1/ COMBINE RAW FILES TOGETHER AND WRITE TO S3

path_combined_files = combine_adminexpress_territory(
    path_within_bucket=path_within_bucket
)

path_raw_s3 = create_path_bucket(
    {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "year": year,
        "borders": "france",
        "crs": 4326,
        "filter_by": "preprocessed",
        "value": "before_cog",
        "vectorfile_format": "geojson",
        "provider": "IGN",
        "dataset_family": "ADMINEXPRESS",
        "source": "EXPRESS-COG-CARTO-TERRITOIRE",
        "territory": "france",
        "filename": "raw.geojson",
        "simplification": 0,
    }
)

fs.put_file(path_combined_files, path_raw_s3)

# Retrieve COG metadata
tagc_metadata = prepare_cog_metadata(path_within_bucket)
tagc_metadata.drop(columns=["LIBGEO"]).to_csv("temp/tagc.csv")


mapshaperize_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "COMMUNE",
        "filter_by": "REGION",
        "simplification": 50,
    }
)

mapshaperize_merge_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "COMMUNE",
        "filter_by": "REGION",
        "simplification": 50,
    }
)

mapshaperize_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "COMMUNE",
        "filter_by": "AIRE_ATTRACTION_VILLES",
        "simplification": 50,
    }
)

mapshaperize_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "BASSIN_VIE",
        "filter_by": "FRANCE_ENTIERE",
        "simplification": 50,
    }
)

mapshaperize_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "ZONE_EMPLOI",
        "filter_by": "TERRITOIRE",
        "simplification": 50,
    }
)

mapshaperize_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "COMMUNE",
        "filter_by": "TERRITOIRE",
        "simplification": 50,
    }
)

mapshaperize_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "COMMUNE",
        "filter_by": "FRANCE_ENTIERE_DROM_RAPPROCHES",
        "simplification": 50,
    }
)

mapshaperize_merge_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "COMMUNE_ARRONDISSEMENT",
        "filter_by": "FRANCE_ENTIERE_DROM_RAPPROCHES",
        "simplification": 50,
    }
)

mapshaperize_merge_split_from_s3(
    {
        "path_within_bucket": path_within_bucket,
        "level_polygons": "BASSIN_VIE",
        "format": "topojson", 
        "filter_by": "FRANCE_ENTIERE_DROM_RAPPROCHES",
        "simplification": 50,
    }
)