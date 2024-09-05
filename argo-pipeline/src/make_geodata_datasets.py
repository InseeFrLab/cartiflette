#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3rd step of pipeline - part 1

Retrieve all territorial cities' files and merge those into single files
for each vintage.
"""

import argparse
import json
import logging
import os
import warnings

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.pipeline.combine_adminexpress_france import (
    combine_adminexpress_territory,
)

logging.basicConfig(level=logging.INFO)


logging.info("=" * 50)
logging.info("\n" + __doc__)
logging.info("=" * 50)

# Initialize ArgumentParser
parser = argparse.ArgumentParser(
    description="Preprocess geodatasets from raw sources"
)
parser.add_argument(
    "-p", "--path", help="Path within bucket", default=PATH_WITHIN_BUCKET
)
parser.add_argument(
    "-lp", "--localpath", help="Path within bucket", default="temp"
)

parser.add_argument(
    "-y", "--years", help="Vintage to perform computation on", default="[]"
)

# Parse arguments
args = parser.parse_args()
path_within_bucket = args.path
local_path = args.localpath
years = args.years

years = json.loads(years)

bucket = BUCKET
fs = FS

os.makedirs(local_path, exist_ok=True)


def main(
    path_within_bucket,
    localpath,
    bucket=BUCKET,
    years: int = None,
):
    # TODO : used only for debugging purposes
    if not years:
        # Perform on all years
        json_md5 = f"{bucket}/{path_within_bucket}/md5.json"
        with fs.open(json_md5, "r") as f:
            all_md5 = json.load(f)
        datasets = all_md5["IGN"]["ADMINEXPRESS"][
            "EXPRESS-COG-CARTO-TERRITOIRE"
        ]
        years = {
            year
            for (_territory, vintaged_datasets) in datasets.items()
            for year in vintaged_datasets.keys()
        }

    format_intermediate = "topojson"

    created = []
    for year in years:
        logging.info("-" * 50)
        logging.info(f"Merging territorial files of cities for {year=}")
        logging.info("-" * 50)

        try:
            # Merge all territorial cities files into a single file
            dset_s3_dir = combine_adminexpress_territory(
                year=year,
                path_within_bucket=path_within_bucket,
                intermediate_dir=localpath,
                format_output=format_intermediate,
                bucket=bucket,
                fs=fs,
            )

            if not dset_s3_dir:
                # No files merged
                continue

            created.append(year)

        except Exception as e:
            warnings.warn(f"geodataset {year=} not created: {e}")
            raise

    with open("geodatasets_years.json", "w") as out:
        json.dump(created, out)

    return created


if __name__ == "__main__":
    data = main(path_within_bucket, localpath=local_path, years=years)
