import argparse
import os

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket
from cartiflette.pipeline.combine_adminexpress_france import (
    combine_adminexpress_territory,
)
from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata

# Initialize ArgumentParser
parser = argparse.ArgumentParser(description="Run Cartiflette pipeline script.")
parser.add_argument(
    "-p", "--path", help="Path within bucket", default=PATH_WITHIN_BUCKET
)
parser.add_argument(
    "-lp", "--localpath", help="Path within bucket", default="temp"
)

# Parse arguments
args = parser.parse_args()

bucket = BUCKET
path_within_bucket = args.path
local_path = args.localpath

year = 2022
fs = FS

os.makedirs(local_path, exist_ok=True)

# PART 1/ COMBINE RAW FILES TOGETHER AND WRITE TO S3


def main(path_within_bucket, localpath, bucket=BUCKET, year=year):

    path_combined_files = combine_adminexpress_territory(
        path_within_bucket=path_within_bucket,
        intermediate_dir=localpath
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
    tagc_metadata = prepare_cog_metadata(
        path_within_bucket, local_dir=localpath)
    tagc_metadata.drop(columns=["LIBGEO"]).to_csv(f"{localpath}/tagc.csv")

    data = {"preprocessed": path_combined_files, "metadata": f"{localpath}/tagc.csv"}

    return data


if __name__ == "__main__":
    main(path_within_bucket, localpath=local_path)
