#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
2nd step of pipeline

Select which geodatasets should be updated (those where raw datasets components
have been re-downloaded) to select downstream steps
"""

import argparse
import logging
import os
import json

from cartiflette.config import (
    DATASETS_HIGH_RESOLUTION,
)
from cartiflette.pipeline_constants import COG_TERRITOIRE, IRIS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logger.info("=" * 50)
logger.info("\n%s", __doc__)
logger.info("=" * 50)

parser = argparse.ArgumentParser(description="Select vintage to update")
parser.add_argument(
    "--download_results",
    type=str,
    default="{}",
    help="Results of download pipeline",
)

args = parser.parse_args()
download_results = args.download_results

download_results = json.loads(download_results)

# Example of download_results
# {"IGN": {"ADMINEXPRESS": {"EXPRESS-COG-TERRITOIRE": {"guadeloupe": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=5490/origin=raw/vectorfile_format=shp/territory=guadeloupe/simplification=0/COMMUNE.shp"]}}}, "martinique": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=5490/origin=raw/vectorfile_format=shp/territory=martinique/simplification=0/COMMUNE.shp"]}}}, "guyane": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=2972/origin=raw/vectorfile_format=shp/territory=guyane/simplification=0/COMMUNE.shp"]}}}, "reunion": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=2975/origin=raw/vectorfile_format=shp/territory=reunion/simplification=0/COMMUNE.shp"]}}}, "mayotte": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=4326/origin=raw/vectorfile_format=shp/territory=mayotte/simplification=0/COMMUNE.shp"]}}}, "metropole": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=2154/origin=raw/vectorfile_format=shp/territory=metropole/simplification=0/COMMUNE.shp"]}}}}}}, "Insee": {"COG": {"DEPARTEMENT": {"france_entiere": {"2024": {"downloaded": false, "paths": null}}}, "REGION": {"france_entiere": {"2024": {"downloaded": false, "paths": null}}}}, "TAGC": {"APPARTENANCE": {"france_entiere": {"2024": {"downloaded": true, "paths": {"table-appartenance-geo-communes-2024": ["projet-cartiflette/test/provider=Insee/dataset_family=TAGC/source=APPARTENANCE/year=2024/administrative_level=None/crs=None/origin=raw/vectorfile_format=xlsx/territory=france_entiere/simplification=0/table-appartenance-geo-communes-2024.xlsx"]}}}}}}}


if os.environ.get("ENVIRONMENT", None) == "dev":
    logging.warning("dev environment -> force generation of only 2023 & 2024")


def store_to_json(name, years):
    "util function to store vintage selections to json for argo results"
    with open(name, "w", encoding="utf8") as out:
        json.dump(years, out)
    return years


def filter_geodata(results):
    "filter the downloaded vintages of geodatasets"
    if os.environ.get("ENVIRONMENT", None) == "dev":
        return store_to_json("geodatasets_years.json", [2023, 2024])

    years = set()
    keys_geo = (
        ("ADMINEXPRESS", COG_TERRITOIRE[DATASETS_HIGH_RESOLUTION]),
        ("IRIS", IRIS[DATASETS_HIGH_RESOLUTION]),
    )
    try:
        raw = [results["IGN"][family][geo] for family, geo in keys_geo]
    except KeyError:
        years = []
    else:
        for dset in raw:
            for dict_results in dset.values():
                for year, dict_results_this_year in dict_results.items():
                    if dict_results_this_year["downloaded"]:
                        years.add(year)

    years = sorted(list(years))
    logger.info("selected downstream geodatasets : %s", years)
    return store_to_json("geodatasets_years.json", years)


def filter_metadata(results):
    "filter the downloaded vintages of metadatasets"
    if os.environ.get("ENVIRONMENT", None) == "dev":
        return store_to_json("metadata_years.json", [2023, 2024])

    years = set()
    try:
        raw = [dset for provider, dset in results.items() if provider != "IGN"]
    except KeyError:
        years = []
    else:

        for dset_provider in raw:
            for dset_family in dset_provider.values():
                for dset in dset_family.values():
                    for dict_results in dset.values():
                        for (
                            year,
                            dict_results_this_year,
                        ) in dict_results.items():
                            if dict_results_this_year["downloaded"]:
                                years.add(year)

    years = sorted(list(years))
    logger.info("selected downstream metadatasets : %s", years)
    return store_to_json("metadata_years.json", years)


if __name__ == "__main__":
    filter_geodata(download_results)
    filter_metadata(download_results)
