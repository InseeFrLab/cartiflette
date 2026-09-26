"""Step 3: run one split job and upload its GeoJSON and GeoParquet files."""

import argparse
import logging
import os

from cartiflette import config
from cartiflette.pipeline import split_and_upload
from cartiflette.s3 import get_fs

parser = argparse.ArgumentParser(description="Split and upload one job")
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--inputs", type=str, required=True, help="Prepared inputs dir")
parser.add_argument("--level_polygons", type=str, required=True)
parser.add_argument("--filter_by", type=str, required=True)
parser.add_argument("--simplification", type=float, required=True)
parser.add_argument("--crs", type=int, required=True)
parser.add_argument(
    "--path", type=str, default=config.WRITE_PATH, help="Path in bucket"
)
parser.add_argument("--bucket", type=str, default=config.WRITE_BUCKET)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    split_and_upload(
        year=args.year,
        inputs_dir=args.inputs,
        work_dir=os.path.join(
            "work",
            f"{args.level_polygons}_{args.filter_by}_{args.simplification}_{args.crs}",
        ),
        level_polygons=args.level_polygons,
        filter_by=args.filter_by,
        simplification=args.simplification,
        crs=args.crs,
        fs=get_fs(),
        bucket=args.bucket,
        path_within_bucket=args.path,
    )
