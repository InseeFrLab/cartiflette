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
from typing import List
import warnings

from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
    PIPELINE_SIMPLIFICATION_LEVELS,
)
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
    "-y", "--years", help="Vintage to perform computation on", default="[]"
)

parser.add_argument(
    "-s",
    "--simplify",
    help="Simplifications levels to perform",
    default=PIPELINE_SIMPLIFICATION_LEVELS,
)

# Parse arguments
args = parser.parse_args()
path_within_bucket = args.path
years = args.years
simplifications = args.simplify

years = json.loads(years)

bucket = BUCKET
fs = FS


def main(
    path_within_bucket,
    simplifications: List[int],
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

    format_intermediate = "geopackage"

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
                format_output=format_intermediate,
                bucket=bucket,
                fs=fs,
                simplifications_values=simplifications,
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
    data = main(
        path_within_bucket, simplifications=simplifications, years=years
    )
