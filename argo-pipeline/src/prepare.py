"""Step 1: download the sources of a year and prepare the pipeline inputs."""

import argparse
import logging

from cartiflette.pipeline import prepare_year

parser = argparse.ArgumentParser(description="Prepare cartiflette inputs")
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--localpath", type=str, required=True, help="Output directory")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    prepare_year(args.year, args.localpath)
