"""Step 2: print the jobs to run as JSON (consumed by Argo `withParam`)."""

import argparse
import json

from cartiflette.pipeline import combinations, consolidated_combinations

parser = argparse.ArgumentParser(description="List the jobs")
parser.add_argument(
    "--kind",
    choices=["geojson", "parquet"],
    default="geojson",
    help="geojson: one file per value; parquet: one consolidated file per level",
)
parser.add_argument(
    "--restrictfield",
    type=str,
    default=None,
    help="Only keep the jobs for this level of polygons",
)

if __name__ == "__main__":
    args = parser.parse_args()
    levels = [args.restrictfield] if args.restrictfield else None
    list_jobs = combinations if args.kind == "geojson" else consolidated_combinations
    jobs = [
        {key.replace("_", "-"): value for key, value in job.items()}
        for job in list_jobs(levels)
    ]
    print(json.dumps(jobs))
