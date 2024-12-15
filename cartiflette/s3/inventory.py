# -*- coding: utf-8 -*-
"""
Created on Sat Dec 14 19:18:05 2024
"""

import logging
import re

import pandas as pd
from s3fs import S3FileSystem

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET

logger = logging.getLogger(__name__)


def make_s3_inventory(
    fs: S3FileSystem = FS,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
):

    # paths = (
    #     f"{bucket}/{path_within_bucket}/"
    #     "provider=Cartiflette/dataset_family=production/"
    #     "**/*"
    # )
    # debug
    paths = (
        f"{bucket}/{path_within_bucket}/"
        "provider=Cartiflette/dataset_family=production/"
        "*CONTOUR-IRIS/**/*001*/**/*"
    )

    paths = fs.glob(paths)
    print(paths)

    compiled = re.compile(
        ".*?/"
        "source=(?P<source>.*?)/"
        "year=(?P<year>[0-9]{4})/"
        "administrative_level=(?P<administrative_level>.*?)/"
        "crs=(?P<crs>[0-9]{4})/"
        "(?P<filter_by>.*?)=(?P<value>.*?)/"
        "vectorfile_format=(?P<vectorfile_format>.*?)/"
        "territory=(?P<territory>.*?)/"
        "simplification=(?P<simplification>[0-9]*?)/"
        ".*"
    )

    datasets = [next(compiled.finditer(path).groupdict()) for path in paths]
    datasets = pd.DataFrame(datasets)

    with fs.open(
        f"{bucket}/{path_within_bucket}/inventory.json", "w", encoding="utf8"
    ) as f:
        datasets.to_json(f, orient="records")

    return datasets


if __name__ == "__main__":
    df = make_s3_inventory()
