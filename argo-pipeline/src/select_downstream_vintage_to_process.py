#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
2nd step of pipeline

Select which geodatasets should be updated (those where raw datasets components
have been re-downloaded) to select downstream steps
"""

import argparse
import json

print("=" * 50)
print(__doc__)
print("=" * 50)

parser = argparse.ArgumentParser(
    description="Select vintage geodatasets to update"
)
parser.add_argument(
    "--download_results",
    type=str,
    default=None,
    help="Results of download pipeline",
)

args = parser.parse_args()
download_results = args.download_results

# Example of download_results
# {"IGN": {"ADMINEXPRESS": {"EXPRESS-COG-TERRITOIRE": {"guadeloupe": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=5490/origin=raw/vectorfile_format=shp/territory=guadeloupe/simplification=0/COMMUNE.shp"]}}}, "martinique": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=5490/origin=raw/vectorfile_format=shp/territory=martinique/simplification=0/COMMUNE.shp"]}}}, "guyane": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=2972/origin=raw/vectorfile_format=shp/territory=guyane/simplification=0/COMMUNE.shp"]}}}, "reunion": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=2975/origin=raw/vectorfile_format=shp/territory=reunion/simplification=0/COMMUNE.shp"]}}}, "mayotte": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=4326/origin=raw/vectorfile_format=shp/territory=mayotte/simplification=0/COMMUNE.shp"]}}}, "metropole": {"2024": {"downloaded": true, "paths": {"COMMUNE": ["projet-cartiflette/test/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/year=2024/administrative_level=None/crs=2154/origin=raw/vectorfile_format=shp/territory=metropole/simplification=0/COMMUNE.shp"]}}}}}}, "Insee": {"COG": {"DEPARTEMENT": {"france_entiere": {"2024": {"downloaded": false, "paths": null}}}, "REGION": {"france_entiere": {"2024": {"downloaded": false, "paths": null}}}}, "TAGC": {"APPARTENANCE": {"france_entiere": {"2024": {"downloaded": true, "paths": {"table-appartenance-geo-communes-2024": ["projet-cartiflette/test/provider=Insee/dataset_family=TAGC/source=APPARTENANCE/year=2024/administrative_level=None/crs=None/origin=raw/vectorfile_format=xlsx/territory=france_entiere/simplification=0/table-appartenance-geo-communes-2024.xlsx"]}}}}}}}

# Only need to update geodata if IGN files fom EXPRESS-COG-TERRITOIRE have been
# updated
years_geodata = set()
try:
    raw_geodatasets = download_results["IGN"]["ADMINEXPRESS"][
        "EXPRESS-COG-TERRITOIRE"
    ]
except KeyError:
    years_geodata = []
else:
    for _territory, dict_results in raw_geodatasets.items():
        for year, dict_results_this_year in dict_results.items():
            if dict_results_this_year["downloaded"]:
                years_geodata.add(year)

    years_geodata = sorted(list(years_geodata))
finally:
    with open("geodatasets_years.json", "w") as out:
        json.dump(years_geodata, out)


years_metadata = set()
try:
    raw_datasets = download_results["Insee"]["COG"]
except KeyError:
    years_metadata = []
else:
    targets = ["DEPARTEMENT", "REGION", "TAGC"]
    for target in targets:
        try:
            for _territory, dict_results in raw_datasets[target].items():
                for year, dict_results_this_year in dict_results.items():
                    if dict_results_this_year["downloaded"]:
                        years_metadata.add(year)
        except KeyError:
            continue
finally:
    years_metadata = sorted(list(years_metadata))

with open("metadata_years.json", "w") as out:
    json.dump(years_metadata, out)
