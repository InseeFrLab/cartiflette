#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
First step of pipeline

Performs a full pipeline to download data and store them on MinIO. The
target files are described in cartiflette/constants.py under the
constant DOWNLOAD_PIPELINE_ARGS. Those files' characteristics must also be
described in the cartiflette/utils/sources.yaml file.

Note: to perform an easy debugging task, please overwrite
cartiflette.config.THREADS_DOWNLOAD to 1 (to avoid multithreading which
could be gruesome to debug).

During the operation:
    * GIS files should be reprojected to 4326 if curent projection has no EPSG
      code
    * each file should be re-encoded in UTF-8

"""

import argparse
import os
import json

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.download import download_all

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
    "--years",
    type=str,
    help="List of years to perform download on (as comma separated values)",
    default=None,
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
local_path = args.localpath
years = args.years
skip = args.skip

if years:
    years = [int(x) for x in years.split(",")]

fs = FS

os.makedirs(local_path, exist_ok=True)

if not skip:
    try:
        results = download_all(
            bucket, path_within_bucket, fs=fs, upload=True, years=years
        )

        with open("download_all_results.json", "w") as out:
            json.dump(results, out)
    except Exception:
        try:
            os.unlink("download_all_results.json")
        except FileNotFoundError:
            pass
        raise

    print(results)

else:
    print(
        "Download skipped! "
        "To reset download, remove --skip flag from pipeline yaml (from download-all-sources)!"
    )
