#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
4.2th step of pipeline

Prepare arguments for next step
"""

import argparse
import json
import logging
import os
from typing import List

from s3fs import S3FileSystem

from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
)
from cartiflette.pipeline_constants import (
    PIPELINE_SIMPLIFICATION_LEVELS,
    # PIPELINE_FORMATS,
    # PIPELINE_CRS,
)

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

logger.info("=" * 50)
logger.info("\n" + __doc__)
logger.info("=" * 50)


parser = argparse.ArgumentParser(description="Crossproduct Script")

parser.add_argument(
    "-y",
    "--year",
    default="2023",
    help="Filter downstream vintage to process",
)

# parser.add_argument(
#     "-f",
#     "--formats",
#     default=",".join(PIPELINE_FORMATS),
#     help="Desired output formats, as a comma separated values list",
# )

# parser.add_argument(
#     "-c",
#     "--crs",
#     default=",".join([str(x) for x in PIPELINE_CRS]),
#     help="Desired projections as EPSG codes, as a comma separated values list",
# )

parser.add_argument(
    "-s",
    "--simplifications",
    default=",".join([str(x) for x in PIPELINE_SIMPLIFICATION_LEVELS]),
    help="Desired simplifications levels, as a comma separated values list",
)


args = parser.parse_args()

year = args.year
# formats = args.formats.split(",")
# crs = args.crs.split(",")
simplifications = args.simplifications.split(",")


# TODO : convert bucket & path_within_bucket to parsable arguments


def main(
    year: int = None,
    simplifications: List[str] = None,
    formats: List[str] = None,
    crs: List[int] = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: S3FileSystem = FS,
):

    simplifications = (
        simplifications if simplifications else PIPELINE_SIMPLIFICATION_LEVELS
    )

    logger.info("Crossproduct with year=%s", year)
    logger.info("Crossproduct with simplifications=%s", simplifications)
    logger.info("Crossproduct with formats=%s", formats)
    logger.info("Crossproduct with crs=%s", crs)

    configs = crossproduct_parameters_production(
        # list_format=formats,
        year=year,
        # crs_list=crs,
        simplifications=simplifications,
        fs=fs,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
    )

    try:
        os.makedirs("configs_datasets_to_generate")
    except FileExistsError:
        pass

    with open(f"configs_datasets_to_generate/{year}.json", "w") as out:
        json.dump(configs, out)
    return configs


if __name__ == "__main__":
    configs = main(
        year=year,
        simplifications=simplifications,
        bucket=BUCKET,
        path_within_bucket=PATH_WITHIN_BUCKET,
        fs=FS,
    )
