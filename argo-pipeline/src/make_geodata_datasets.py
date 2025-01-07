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
from typing import List

from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
    INTERMEDIATE_FORMAT,
)
from cartiflette.pipeline_constants import PIPELINE_SIMPLIFICATION_LEVELS
from cartiflette.pipeline.prepare_geodatasets import (
    create_one_year_geodataset_batch,
)

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
    "-y", "--year", help="Vintage to perform computation on", default="2023"
)

parser.add_argument(
    "-s",
    "--simplify",
    help="Simplifications levels to perform",
    default=PIPELINE_SIMPLIFICATION_LEVELS,
)

# Parse arguments
args = parser.parse_args()
year = args.year
simplifications = args.simplify

bucket = BUCKET
fs = FS


def main(
    simplifications: List[int],
    bucket=BUCKET,
    year: int = None,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
):

    created = create_one_year_geodataset_batch(
        year,
        format_output=INTERMEDIATE_FORMAT,
        simplifications_values=simplifications,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        fs=fs,
    )

    try:
        os.makedirs("geodataset_years")
    except FileExistsError:
        pass

    with open(f"geodataset_years/{year}.json", "w") as out:
        json.dump(created, out)

    return created


if __name__ == "__main__":
    data = main(simplifications=simplifications, year=year)
