#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Last step of pipeline

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
    DATASETS_HIGH_RESOLUTION,
)
from cartiflette.pipeline import mapshaperize_split_from_s3
from cartiflette.pipeline_constants import COG_TERRITOIRE

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
    "-lp",
    "--level_polygons_init",
    default="ARRONDISSEMENT_MUNICIPAL",
    help="Initial level of geometries (=mesh)",
)
parser.add_argument(
    "-so",
    "--source",
    default=COG_TERRITOIRE[DATASETS_HIGH_RESOLUTION],
    help="Select upstream raw source to use for geometries",
)
parser.add_argument(
    "-si",
    "--simplification",
    default="40",
    help="Desired simplification level",
)
parser.add_argument(
    "-d",
    "--dissolve_by",
    default="DEPARTEMENT",
    help="Desired geometry dissolution level",
)
parser.add_argument(
    "-c",
    "--config_generation",
    default='{"FRANCE_ENTIERE": [{"format_output": "gpkg", "epsg": "4326"}]}',
    help="Desired split level",
)

# Parse the arguments
args = parser.parse_args()


def main(
    year,
    init_geometry_level,
    source,
    simplification,
    dissolve_by,
    config_generation: dict,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: S3FileSystem = FS,
):

    result = mapshaperize_split_from_s3(
        year=year,
        init_geometry_level=init_geometry_level,
        source=source,
        simplification=simplification,
        dissolve_by=dissolve_by,
        config_generation=config_generation,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        fs=fs,
    )

    out_path = (
        f"{year}/{init_geometry_level}/{source}/{simplification}/{dissolve_by}"
        "/result.json"
    )
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
        init_geometry_level=args.level_polygons_init,
        source=args.source,
        simplification=args.simplification,
        dissolve_by=args.dissolve_by,
        config_generation=json.loads(args.config_generation),
        bucket=BUCKET,
        path_within_bucket=PATH_WITHIN_BUCKET,
        fs=FS,
    )
