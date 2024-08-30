#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3rd step of pipeline - part 1

Retrieve all territorial cities' files and merge those into single files
for each vintage.
"""

import argparse
import json
import os
import shutil

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket

from cartiflette.pipeline.combine_adminexpress_france import (
    combine_adminexpress_territory,
)

from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata

print("=" * 50)
print(__doc__)
print("=" * 50)

# Initialize ArgumentParser
parser = argparse.ArgumentParser(
    description="Run Cartiflette pipeline script."
)
parser.add_argument(
    "-p", "--path", help="Path within bucket", default=PATH_WITHIN_BUCKET
)
parser.add_argument(
    "-lp", "--localpath", help="Path within bucket", default="temp"
)

parser.add_argument(
    "-y", "--years", help="Vintage to perform computation on", default=None
)

# Parse arguments
args = parser.parse_args()

bucket = BUCKET
path_within_bucket = args.path
local_path = args.localpath
years = args.years

years = json.loads(years)

fs = FS

os.makedirs(local_path, exist_ok=True)

# PART 1/ COMBINE RAW FILES TOGETHER AND WRITE TO S3


def main(
    path_within_bucket,
    localpath,
    bucket=BUCKET,
    years: int = None,
):
    if not years:
        # Perform on all years
        json_md5 = f"{bucket}/{path_within_bucket}/md5.json"
        with fs.open(json_md5, "r") as f:
            all_md5 = json.load(f)
        datasets = all_md5["IGN"]["ADMINEXPRESS"]["EXPRESS-COG-TERRITOIRE"]
        years = {
            year
            for (_territory, vintaged_datasets) in datasets.items()
            for year in vintaged_datasets.keys()
        }

    format_intermediate = "topojson"

    created = []
    for year in years:
        print("-" * 50)
        print(f"Merging territorial files of cities for {year=}")
        print("-" * 50)

        try:
            # Merge all territorial cities files into a single file
            path_combined_files = combine_adminexpress_territory(
                year=year,
                path_within_bucket=path_within_bucket,
                intermediate_dir=localpath,
                format_intermediate=format_intermediate,
                bucket=bucket,
                fs=fs,
            )

            if not path_combined_files:
                # No files merged
                continue

            # Upload file to S3 file system
            path_raw_s3 = create_path_bucket(
                {
                    "bucket": bucket,
                    "path_within_bucket": path_within_bucket,
                    "year": year,
                    "borders": "france",
                    "crs": 4326,
                    "filter_by": "preprocessed",
                    "value": "before_cog",
                    "vectorfile_format": format_intermediate,
                    "provider": "IGN",
                    "dataset_family": "ADMINEXPRESS",
                    "source": "EXPRESS-COG-CARTO-TERRITOIRE",
                    "territory": "france",
                    "filename": f"raw.{format_intermediate}",
                    "simplification": 0,
                }
            )

            fs.put_file(path_combined_files, path_raw_s3)

            created.append(path_raw_s3)

        except Exception:
            raise
        finally:
            # clean up tempfiles whatever happens
            shutil.rmtree(localpath, ignore_errors=True)

    return created


if __name__ == "__main__":
    data = main(path_within_bucket, localpath=local_path, years=years)
