#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Create cartiflette's catalog
"""

import json
import logging

from s3fs import S3FileSystem

from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
)
from cartiflette.s3 import make_s3_inventory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logger.info("=" * 50)
logger.info("\n%s", __doc__)
logger.info("=" * 50)

# Nota : no parsed needed for this command


def main(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: S3FileSystem = FS,
):

    make_s3_inventory(
        fs=fs, bucket=bucket, path_within_bucket=path_within_bucket
    )

    logger.info("Success!")


if __name__ == "__main__":
    main(
        bucket=BUCKET,
        path_within_bucket=PATH_WITHIN_BUCKET,
        fs=FS,
    )
