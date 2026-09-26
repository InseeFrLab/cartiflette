"""
Writing to S3. Every upload goes through `upload`, which refuses to write to
the production prefix unless explicitly allowed (see cartiflette.config).
"""

from __future__ import annotations

import logging
import os

import s3fs

from cartiflette import config

logger = logging.getLogger(__name__)


def get_fs() -> s3fs.S3FileSystem:
    """S3 filesystem using the AWS_* credentials of the environment."""
    return s3fs.S3FileSystem(client_kwargs={"endpoint_url": config.ENDPOINT_URL})


def check_write_target(bucket: str, path_within_bucket: str) -> None:
    """Raise if the target is the production prefix and this is not allowed."""
    is_production = (
        bucket == config.PRODUCTION_BUCKET
        and path_within_bucket.strip("/").split("/")[0] == config.PRODUCTION_PATH
    )
    if is_production and not config.ALLOW_PRODUCTION_WRITE:
        raise PermissionError(
            f"Refusing to write to {bucket}/{path_within_bucket}: this is the "
            "production prefix read by the clients. Set CARTIFLETTE_WRITE_PATH "
            "to a test location."
        )


def upload(
    local_path: str,
    remote_path: str,
    fs: s3fs.S3FileSystem,
) -> str:
    """Upload a single file to `remote_path` ("bucket/path/.../file.ext")."""
    bucket, _, path_within_bucket = remote_path.partition("/")
    check_write_target(bucket, path_within_bucket)
    logger.info("upload %s -> %s", os.path.basename(local_path), remote_path)
    fs.put_file(local_path, remote_path)
    return remote_path
