#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3rd step of pipeline - part 2

Update/create vintaged metadata files and send those to S3
"""

import argparse
import json
import os

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket


from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata

print("=" * 50)
print(__doc__)
print("=" * 50)

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
    "-y", "--years", help="Vintage to perform computation on", default=None
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
    localpath,
    bucket=BUCKET,
    years: int = None,
):
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
    for year in years:
        print("-" * 50)
        print(f"Computing metadata for {year=}")
        print("-" * 50)

        os.makedirs(f"{local_path}/{year}", exist_ok=True)

        try:
            path_raw_s3 = create_path_bucket(
                {
                    "bucket": bucket,
                    "path_within_bucket": path_within_bucket,
                    "year": year,
                    "borders": "france",
                    "crs": None,
                    "filter_by": "preprocessed",
                    "value": "tagc",
                    "vectorfile_format": "csv",
                    "provider": "Insee",
                    "dataset_family": "COG-TAGC",
                    "source": "COG-TAGC",
                    "territory": "france",
                    "filename": "tagc.csv",
                    "simplification": 0,
                }
            )

            # Retrieve COG metadata
            tagc_metadata = prepare_cog_metadata(
                bucket=bucket,
                path_within_bucket=path_within_bucket,
                year=year,
            )
            if tagc_metadata is None:
                continue
            tagc_metadata.drop(columns=["LIBGEO"]).to_csv(
                f"{localpath}/{year}/tagc.csv"
            )
            fs.put_file(f"{localpath}/{year}/tagc.csv", path_raw_s3)

            created.append(year)

        except Exception:
            raise

        finally:
            # clean up tempfiles whatever happens
            try:
                os.unlink(f"{localpath}/{year}/tagc.csv")
            except FileNotFoundError:
                # generation failed
                pass

    with open("metadata_years.json", "w") as out:
        json.dump(created, out)

    return created


if __name__ == "__main__":
    data = main(path_within_bucket, localpath=local_path, years=years)
