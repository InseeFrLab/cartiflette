"""Module for communication with Minio S3 Storage
"""

from typing import Optional

from cartiflette import BUCKET, PATH_WITHIN_BUCKET


# CREATE STANDARDIZED PATHS ------------------------


class ConfigDict:
    bucket: Optional[str]
    path_within_bucket: Optional[str]
    provider: str
    source: str
    vectorfile_format: str
    borders: str
    filter_by: str
    year: str
    crs: Optional[int]
    value: str


def create_path_bucket(config: ConfigDict) -> str:
    """
    This function creates a file path for a vector file within a specified
    bucket.

    Parameters
    ----------
    config : ConfigDict
        A dictionary containing vector file parameters.

    Returns
    -------
    str
       The complete file path for the vector file that will be used to read
       or write when interacting with S3 storage.

    """

    bucket = config.get("bucket", BUCKET)
    path_within_bucket = config.get("path_within_bucket", PATH_WITHIN_BUCKET)
    provider = config.get("provider")
    source = config.get("source")
    vectorfile_format = config.get("vectorfile_format")
    borders = config.get("borders")
    filter_by = config.get("filter_by")
    year = config.get("year")
    value = config.get("value")
    crs = config.get("crs", 2154)

    write_path = (
        f"{bucket}/{path_within_bucket}"
        f"/{year=}"
        f"/administrative_level={borders}"
        f"/{crs=}"
        f"/{filter_by}={value}/{vectorfile_format=}"
        f"/{provider=}/{source=}"
        f"/raw.{vectorfile_format}"
    ).replace("'", "")

    if vectorfile_format == "shp":
        write_path = write_path.rsplit("/", maxsplit=1)[0] + "/"

    return write_path
