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

logging.info("=" * 50)
logging.info("\n%s", __doc__)
logging.info("=" * 50)

# Initialize ArgumentParser
parser = argparse.ArgumentParser(
    description="Preprocess metadata from raw sources"
)
parser.add_argument(
    "-p", "--path", help="Path within bucket", default=PATH_WITHIN_BUCKET
)
parser.add_argument(
    "-lp", "--localpath", help="Local temporary file", default="temp"
)

parser.add_argument(
    "-y", "--years", help="Vintage to perform computation on", default="[]"
)

# Parse arguments
args = parser.parse_args()

bucket = BUCKET
path_within_bucket = args.path
local_path = args.localpath
years = args.years

years = json.loads(years)

fs = FS

os.makedirs(local_path, exist_ok=True)


def main(
    path_within_bucket,
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

    created = []

    with tempfile.TemporaryDirectory() as tempdir:
        for year in years:
            logging.info("-" * 50)
            logging.info("Computing metadata for year=%s", year)
            logging.info("-" * 50)

            os.makedirs(f"{local_path}/{year}", exist_ok=True)

            config = {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "borders": "COMMUNE",
                "crs": None,
                "filter_by": "preprocessed",
                "value": "tagc",
                "vectorfile_format": "csv",
                "provider": "Cartiflette",
                "dataset_family": "metadata",
                "source": "COG-TAGC",
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

            for key in ["COMMUNE", "CANTON"]:
                try:
                    metadata_border = metadata[key]
                except KeyError:
                    continue
                if metadata_border is None:
                    continue
                config["borders"] = key
                path_raw_s3 = create_path_bucket(config)
                localfile = f"{tempdir}/metadata.csv"
                metadata_border.to_csv(localfile)
                try:
                    logging.info("sending %s -> %s", localfile, path_raw_s3)
                    fs.put_file(localfile, path_raw_s3)
                except Exception:
                    raise
                finally:
                    os.unlink(localfile)

                # if at least one metadata constructed
                created.append(year)

    with open("metadata_years.json", "w", encoding="utf8") as out:
        json.dump(created, out)

    return created


if __name__ == "__main__":
    data = main(path_within_bucket, years=years)
