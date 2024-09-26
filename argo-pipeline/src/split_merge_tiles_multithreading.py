#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Last step of pipeline (with multithreading)

Create all geodatasets served by cartiflette
"""

import argparse
import json
import logging
import os

from s3fs import S3FileSystem

from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
)
from cartiflette.pipeline import mapshaperize_split_from_s3_multithreading

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logger.info("=" * 50)
logger.info("\n" + __doc__)
logger.info("=" * 50)

parser = argparse.ArgumentParser(description="Process command line arguments.")

# Define the arguments with their default values
parser.add_argument(
    "-y",
    "--year",
    default="2023",
    help="Filter downstream vintage to process",
)

parser.add_argument(
    "-c",
    "--config_generation",
    default='[{"mesh_init":"ARRONDISSEMENT_MUNICIPAL","source_geodata":"EXPRESS-COG-TERRITOIRE","simplification":"0","dissolve_by":"ARRONDISSEMENT_MUNICIPAL","config":{"AIRE_ATTRACTION_VILLES":[{"format_output":"topojson","epsg":"4326"}],"ARRONDISSEMENT":[{"format_output":"topojson","epsg":"4326"}]}}]',
    help="Desired split level",
)

# Parse the arguments
args = parser.parse_args()


def main(
    year,
    config_generation: dict,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: S3FileSystem = FS,
):

    result = mapshaperize_split_from_s3_multithreading(
        year=year,
        config_generation=config_generation,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        fs=fs,
    )

    out_path = f"generation/{year}/result.json"
    try:
        os.makedirs(os.path.dirname(out_path))
    except FileExistsError:
        pass
    with open(out_path, "w") as out:
        json.dump(result, out)

    return result


if __name__ == "__main__":
    main(
        year=args.year,
        config_generation=json.loads(args.config_generation),
        bucket=BUCKET,
        path_within_bucket=PATH_WITHIN_BUCKET,
        fs=FS,
    )
