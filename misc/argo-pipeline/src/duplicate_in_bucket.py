import argparse
import os

from cartiflette import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket
from cartiflette.pipeline.combine_adminexpress_france import (
    combine_adminexpress_territory,
)
from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata

from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.pipeline import (
    mapshaperize_split_from_s3,
    mapshaperize_merge_split_from_s3,
)


# Initialize ArgumentParser
parser = argparse.ArgumentParser(description="Run Cartiflette pipeline script.")
parser.add_argument("-p", "--path", help="Path within bucket", default=PATH_WITHIN_BUCKET)

# Parse arguments
args = parser.parse_args()

bucket = BUCKET
path_within_bucket = args.path

year = 2022
fs = FS

os.makedirs("tmp", exists_ok=True)

# PART 1/ COMBINE RAW FILES TOGETHER AND WRITE TO S3

def main(
    path_within_bucket, bucket=BUCKET, year=year
):

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
    tagc_metadata.drop(columns=["LIBGEO"]).to_csv("tmp/tagc.csv")

    data = {
        "preprocessed": path_combined_files,
        "metadata": "tmp/tagc.csv"
    }

    import os
    print(
        os.getcwd()
    )
    print(
        os.listdir("tmp")
    )


if __name__ == "__main__":
    main(path_within_bucket)