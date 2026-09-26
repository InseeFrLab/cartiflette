"""Step 0: check the write target before anything is downloaded or processed."""

import argparse
import logging

from cartiflette import config
from cartiflette.s3 import check_write_target

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="Check the write target")
parser.add_argument(
    "--path", type=str, default=config.WRITE_PATH, help="Path in bucket"
)
parser.add_argument("--bucket", type=str, default=config.WRITE_BUCKET)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    # Raises PermissionError for the production prefix, unless allowed
    check_write_target(args.bucket, args.path)
    logger.info(
        "Writing to %s/%s%s",
        args.bucket,
        args.path,
        " (PRODUCTION)" if config.ALLOW_PRODUCTION_WRITE else "",
    )
