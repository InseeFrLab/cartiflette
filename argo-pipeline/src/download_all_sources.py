#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
First step of pipeline

Performs a full pipeline to download data and store them on MinIO. The
target files are described in cartiflette/constants.py under the
constant PIPELINE_DOWNLOAD_ARGS. Those files' characteristics must also be
described in the cartiflette/utils/sources.yaml file.

Note: to perform an easy debugging task, please overwrite
cartiflette.config.THREADS_DOWNLOAD to 1 (to avoid multithreading which
could be gruesome to debug).

During the operation:
    * GIS files should be reprojected to 4326 if current projection has no EPSG
      code
    * each file should be re-encoded in UTF-8
    * unvalid geometries will try to be fixed using a 0 buffer

"""

import argparse
from datetime import date
import logging
import os
import json

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.pipeline import download_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("=" * 50)
logger.info("\n" + __doc__)
logger.info("=" * 50)


# Initialize ArgumentParser
parser = argparse.ArgumentParser(
    description="Run Cartiflette pipeline download script."
)
parser.add_argument(
    "-p", "--path", help="Path within bucket", default=PATH_WITHIN_BUCKET
)

default_years = ",".join(str(x) for x in range(2020, date.today().year + 1))
parser.add_argument(
    "--years",
    type=str,
    help="List of years to perform download on (as comma separated values)",
    default=default_years,
)

parser.add_argument(
    "--skip",
    action="store_true",
    help="Skip download for speeding debugging purposes",
)

# Parse arguments
args = parser.parse_args()

bucket = BUCKET
path_within_bucket = args.path
years = args.years
skip = args.skip

if os.environ.get("ENVIRONMENT", None) == "dev":
    logging.warning("dev environment -> restrict download to 2023 & 2024 only")
    years = "2023,2024"

if years:
    years = [int(x) for x in years.split(",")]

fs = FS


try:
    if not skip:
        results = download_all(
            bucket, path_within_bucket, fs=fs, upload=True, years=years
        )
    else:
        results = dict()
        logger.warning(
            "\n\n!!!! Download skipped !!!\n\n"
            "To reset download, remove --skip flag from pipeline yaml (from "
            "download-all-sources template)!"
        )

    with open("download_all_results.json", "w") as out:
        json.dump(results, out)
except Exception:
    try:
        os.unlink("download_all_results.json")
    except FileNotFoundError:
        pass
    raise

logger.info(results)
