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

from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
    PIPELINE_SIMPLIFICATION_LEVELS,
)
from cartiflette.pipeline.prepare_geodatasets import make_all_geodatasets

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


logger.info("=" * 50)
logger.info("\n" + __doc__)
logger.info("=" * 50)

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

    created = make_all_geodatasets(
        years,
        format_output="geojson",
        simplifications_values=simplifications,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        fs=fs,
    )

    with open("geodatasets_years.json", "w") as out:
        json.dump(created, out)

    return created


if __name__ == "__main__":
    data = main(
        path_within_bucket, simplifications=simplifications, years=years
    )
