"""Module for communication with Minio S3 Storage
"""

from typing import Dict, Union

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "diffusion/shapefiles-test1"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

# CREATE STANDARDIZED PATHS ------------------------


def create_path_bucket(config: Dict[str, Union[str, int, float]]) -> str:
    """
    This function creates a file path for a vector file within a specified bucket.

    Parameters:
    config (Dict[str, Union[str, int, float]]): A dictionary containing vector file parameters.

    Returns:
    str: The complete file path for the vector file that will be used to read
    or write when interacting with S3 storage.
    """

    bucket = config.get("bucket", BUCKET)
    path_within_bucket = config.get("path_within_bucket", PATH_WITHIN_BUCKET)
    provider = config.get("provider", "IGN")
    source = config.get("source", "EXPRESS-COG-TERRITOIRE")
    vectorfile_format = config.get("vectorfile_format", "geojson")
    borders = config.get("borders", "COMMUNE")
    filter_by = config.get("filter_by", "region")
    year = config.get("year", "2022")
    value = config.get("value", "28")
    crs = config.get("crs", 2154)

    write_path = f"{bucket}/{path_within_bucket}"
    write_path = f"{write_path}/{year=}"
    write_path = f"{write_path}/administrative_level={borders}"
    write_path = f"{write_path}/{crs=}"
    write_path = f"{write_path}/{filter_by}={value}/{vectorfile_format=}"
    write_path = f"{write_path}/{provider=}/{source=}"
    write_path = f"{write_path}/raw.{vectorfile_format}"
    write_path = write_path.replace("'", "")

    if vectorfile_format == "shp":
        write_path = write_path.rsplit("/", maxsplit=1)[0] + "/"

    return write_path
