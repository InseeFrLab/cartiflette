#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
4th step of pipeline

Prepare arguments for next step
"""

import argparse
import json
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
    PIPELINE_FORMATS,
    PIPELINE_CRS,
)

parser = argparse.ArgumentParser(description="Crossproduct Script")

parser.add_argument(
    "-yg",
    "--years-geodatasets",
    default="[]",
    help="Updated geodataset's vintages",
)

parser.add_argument(
    "-ym",
    "--years-metadata",
    default="[]",
    help="Updated metadata's vintages",
)

parser.add_argument(
    "-f",
    "--formats",
    default=",".join(PIPELINE_FORMATS),
    help="Desired output formats, as a comma sperated values list",
)

parser.add_argument(
    "-c",
    "--crs",
    default=",".join([str(x) for x in PIPELINE_CRS]),
    help="Desired projections as EPSG codes, as a comma sperated values list",
)

parser.add_argument(
    "-s",
    "--simplifications",
    default=",".join([str(x) for x in PIPELINE_SIMPLIFICATION_LEVELS]),
    help="Desired simplifications levels, as a comma sperated values list",
)


args = parser.parse_args()

years_geodatasets = set(json.loads(args.years_geodatasets))
years_metadata = set(json.loads(args.years_metadata))
formats = args.formats.split(",")
crs = args.crs.split(",")
simplifications = args.simplifications.split(",")

years = sorted(list(years_geodatasets | years_metadata))
years = [int(x) for x in years]

# TODO : convert bucket & path_within_bucket to parsable arguments


# TODO : used only for debugging purposes
if not years:
    # Perform on all years
    json_md5 = f"{BUCKET}/{PATH_WITHIN_BUCKET}/md5.json"
    with FS.open(json_md5, "r") as f:
        all_md5 = json.load(f)
    datasets = all_md5["IGN"]["ADMINEXPRESS"]["EXPRESS-COG-CARTO-TERRITOIRE"]
    years = {
        year
        for (_territory, vintaged_datasets) in datasets.items()
        for year in vintaged_datasets.keys()
    }
    years = list(years)


def main(
    years: List[int] = None,
    simplifications: List[str] = None,
    formats: List[str] = None,
    crs: List[int] = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: S3FileSystem = FS,
):
    # %% TODO : used only for debugging purposes
    if not years:
        # Perform on all COG years
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
    # %%

    simplifications = (
        simplifications if simplifications else PIPELINE_SIMPLIFICATION_LEVELS
    )

    configs = crossproduct_parameters_production(
        list_format=formats,
        years=years,
        crs_list=crs,
        simplifications=simplifications,
    )

    with open("configs_datasets_to_generate.json", "w") as out:
        json.dump(configs, out)
    return configs


if __name__ == "__main__":
    configs = main(
        years=years,
        simplifications=simplifications,
        formats=formats,
        crs=crs,
        bucket=BUCKET,
        path_within_bucket=PATH_WITHIN_BUCKET,
        fs=FS,
    )
