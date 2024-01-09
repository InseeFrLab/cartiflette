import argparse
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

print(args.path)