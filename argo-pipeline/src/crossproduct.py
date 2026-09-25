"""Step 2: print the jobs to run as JSON (consumed by Argo `withParam`)."""

import argparse
import json

from cartiflette.pipeline import combinations

parser = argparse.ArgumentParser(description="List the split jobs")
parser.add_argument(
    "--restrictfield",
    type=str,
    default=None,
    help="Only keep the jobs for this level of polygons",
)

if __name__ == "__main__":
    args = parser.parse_args()
    levels = [args.restrictfield] if args.restrictfield else None
    jobs = [
        {key.replace("_", "-"): value for key, value in job.items()}
        for job in combinations(levels)
    ]
    print(json.dumps(jobs))
