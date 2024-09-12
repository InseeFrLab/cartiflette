#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
4th step of pipeline

Prepare arguments for next step
"""

import json
import argparse
from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
    PIPELINE_SIMPLIFICATION_LEVELS,
)

parser = argparse.ArgumentParser(description="Crossproduct Script")
parser.add_argument(
    "--restrict-field",
    type=str,
    default=None,
    help="Field to restrict level-polygons",
)

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

args = parser.parse_args()

years_geodatasets = set(json.loads(args.years_geodatasets))
years_metadata = set(json.loads(args.years_metadata))

years = sorted(list(years_geodatasets | years_metadata))

# TODO : convert to parsable arguments
bucket = BUCKET
path_within_bucket = PATH_WITHIN_BUCKET
fs = FS

# TODO : used only for debugging purposes
if not years:
    # Perform on all years
    json_md5 = f"{bucket}/{path_within_bucket}/md5.json"
    with fs.open(json_md5, "r") as f:
        all_md5 = json.load(f)
    datasets = all_md5["IGN"]["ADMINEXPRESS"]["EXPRESS-COG-CARTO-TERRITOIRE"]
    years = {
        year
        for (_territory, vintaged_datasets) in datasets.items()
        for year in vintaged_datasets.keys()
    }

# parameters
formats = ["topojson", "geojson"]
crs_list = [4326]
sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]

croisement_decoupage_level = {
    # structure -> niveau geo: [niveau decoupage macro],
    "CANTON": [
        "DEPARTEMENT",
        "REGION",  # zonages administratifs
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "COMMUNE": [
        "BASSIN_VIE",
        "ZONE_EMPLOI",
        "UNITE_URBAINE",
        "AIRE_ATTRACTION_VILLES",  # zonages d'études
        "DEPARTEMENT",
        "REGION",  # zonages administratifs
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "DEPARTEMENT": [
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "REGION": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "BASSIN_VIE": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "ZONE_EMPLOI": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "UNITE_URBAINE": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "AIRE_ATTRACTION_VILLES": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
}


def main(
    path_within_bucket: str,
    bucket: str,
    years: list = None,
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

    tempdf = crossproduct_parameters_production(
        croisement_filter_by_borders=croisement_decoupage_level,
        list_format=formats,
        years=years,
        crs_list=crs_list,
        sources=sources,
        simplifications=PIPELINE_SIMPLIFICATION_LEVELS,
    )
    tempdf.columns = tempdf.columns.str.replace("_", "-")

    # Apply filtering if restrict_field is provided
    if args.restrict_field:
        tempdf = tempdf.loc[tempdf["level-polygons"] == args.restrict_field]

    output = tempdf.to_json(orient="records")
    parsed = json.loads(output)

    print(json.dumps(parsed))


if __name__ == "__main__":
    main(path_within_bucket=path_within_bucket, bucket=bucket, years=years)
