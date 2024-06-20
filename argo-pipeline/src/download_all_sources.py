#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS

from cartiflette.download import download_all

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

# Parse arguments
args = parser.parse_args()

bucket = BUCKET
path_within_bucket = args.path
local_path = args.localpath

fs = FS

os.makedirs(local_path, exist_ok=True)

# PART 1/ COMBINE RAW FILES TOGETHER AND WRITE TO S3
paths = download_all(bucket, path_within_bucket, fs=fs, upload=True)
