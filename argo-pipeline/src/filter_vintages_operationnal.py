#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
4.1th step of pipeline

Filter years for which geodata OR metadata have been successfuly generated.
This is a dummy task in the dag, only used to force a fan-out step, which
should ensure next step does not reach ARGO's maximum 262,144 characters for
output/input.
"""

import argparse
import json
import logging


logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(description="Crossproduct Script")

parser.add_argument(
    "-yg",
    "--years-geodatasets",
    default=r'["{\"2023\": true}"]',
    help="Updated geodataset's vintages",
)

parser.add_argument(
    "-ym",
    "--years-metadata",
    default="[2023]",
    help="Updated metadata's vintages",
)

args = parser.parse_args()

years_geodatasets = [json.loads(x) for x in json.loads(args.years_geodatasets)]
years_geodatasets = {
    int(year)
    for d in years_geodatasets
    for (year, result) in d.items()
    if result
}

years_metadata = {int(x) for x in json.loads(args.years_metadata)}

years = sorted(list(years_geodatasets | years_metadata))

logger.info(
    "selected downstream years for operationnal generation of datasets : %s",
    years,
)

with open("vintages_operationnal_generation.json", "w") as out:
    json.dump(years, out)
