#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
2nd step of pipeline

Select which geodatasets should be updated (those where raw datasets components
have been re-downloaded)
"""

import json

print("=" * 50)
print(__doc__)
print("=" * 50)

with open("download_all_results.json", "r") as f:
    download_results = json.load(f)

# Example of download_results
# {
#     'IGN': {
#         'ADMINEXPRESS': {
#             'EXPRESS-COG-TERRITOIRE': {
#                 'guadeloupe': {
#                     2022: {
#                         'downloaded': True,
#                         'paths': {
#                             'COMMUNE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/COMMUNE.shp']
#                         }
#                     }
#                 },
#                 'metropole': {
#                     2022: {
#                         'downloaded': True,
#                         'paths': {
#                             'COMMUNE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/COMMUNE.shp']
#                         }
#                     }
#                 }
#             }
#         }
#     }
# }

# Only need to update geodata if IGN files fom EXPRESS-COG-TERRITOIRE have been
# updated
years = {}
raw_geodatasets = download_results["IGN"]["ADMIN-EXPRESS"][
    "EXPRESS-COG-TERRITOIRE"
]
for _territory, dict_results in raw_geodatasets.items():
    for year, dict_results_this_year in dict_results.items():
        if dict_results_this_year["downloaded"]:
            years.append(year)

results = sorted(list(years))
print(results)
