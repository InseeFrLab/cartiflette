#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3rd step of pipeline - part 2

Update/create vintaged metadata files and send those to S3
"""

import argparse
import json
import logging
import os
import tempfile

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket
from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata


logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

logger.info("=" * 50)
logger.info("\n%s", __doc__)
logger.info("=" * 50)

# Initialize ArgumentParser
parser = argparse.ArgumentParser(
    description="Preprocess metadata from raw sources"
)
parser.add_argument(
    "-p", "--path", help="Path within bucket", default=PATH_WITHIN_BUCKET
)

parser.add_argument(
    "-y", "--years", help="Vintage to perform computation on", default="[]"
)

# Parse arguments
args = parser.parse_args()

bucket = BUCKET
path_within_bucket = args.path
years = args.years

years = json.loads(years)

fs = FS


def main(
    path_within_bucket,
    bucket=BUCKET,
    years: int = None,
):

    created = []

    with tempfile.TemporaryDirectory() as tempdir:
        for year in years:
            logger.info("-" * 50)
            logger.info("Computing metadata for year=%s", year)
            logger.info("-" * 50)

            config = {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "crs": None,
                "filter_by": "preprocessed",
                "value": "tagc",
                "vectorfile_format": "csv",
                "provider": "Cartiflette",
                "dataset_family": "metadata",
                "source": "INSEE",
                "territory": "france",
                "filename": "metadata.csv",
                "simplification": 0,
            }

            # Retrieve COG metadata
            # TODO : update prepare_cog_metadata to send directly to S3
            metadata = prepare_cog_metadata(
                bucket=bucket,
                path_within_bucket=path_within_bucket,
                year=year,
            )
            if metadata is None:
                continue

            for key in [
                "COMMUNE",
                "ARRONDISSEMENT_MUNICIPAL",
                "CANTON",
                "IRIS",
            ]:
                try:
                    metadata_border = metadata[key]
                except KeyError:
                    continue
                if metadata_border is None:
                    continue
                config["borders"] = key
                path_raw_s3 = create_path_bucket(config)
                localfile = f"{tempdir}/metadata.csv"
                metadata_border.to_csv(localfile, index=False)
                try:
                    logger.info("sending %s -> %s", localfile, path_raw_s3)
                    fs.put_file(localfile, path_raw_s3)
                except Exception:
                    raise
                finally:
                    os.unlink(localfile)

                # if at least one metadata constructed
                created.append(year)

    created = sorted(list(set(created)))

    with open("metadata_years.json", "w", encoding="utf8") as out:
        json.dump(created, out)

    return created


if __name__ == "__main__":
    data = main(path_within_bucket, years=years)
