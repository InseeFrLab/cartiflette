"""Module for communication with Minio S3 Storage
"""

import itertools
from collections import ChainMap
import os
import tempfile
import typing
import s3fs
import pandas as pd
import geopandas as gpd

from cartiflette.utils import (
    keep_subset_geopandas,
    dict_corresp_filter_by,
    create_format_standardized,
    create_format_driver,
    download_pb,
    official_epsg_codes,
)

from cartiflette.download import (
    store_vectorfile_ign,
    get_vectorfile_ign,
    get_vectorfile_communes_arrondissement,
    get_cog_year,
)

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "diffusion/shapefiles-test1"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})

# UTILITIES --------------------------------


def structure_path_raw_ign(c):
    source, field, year, provider = c
    path = store_vectorfile_ign(
        source=source, year=year, field=field, provider=provider
    )
    return {f"{year=}/raw/{provider=}/{source=}/{field=}": path}


# CREATE STANDARDIZED PATHS ------------------------


def create_url_s3(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    vectorfile_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = "2022",
    value: typing.Union[str, int, float] = "28",
    crs: typing.Union[list, str, int, float] = 2154,
) -> str:
    """
    This function creates a URL for a vector file stored in an S3 bucket.

    Parameters:
    bucket (str): The name of the bucket where the file is stored. Default is BUCKET.
    path_within_bucket (str): The path within the bucket where the file is stored. Default is PATH_WITHIN_BUCKET.
    vectorfile_format (str): The format of the vector file, can be "geojson", "topojson", "gpkg" or "shp". Default is "geojson".
    borders (str): The administrative borders of the tiles within the vector file. Can be any administrative borders provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    filter_by (str): The administrative borders (supra to 'borders') that will be used to cut the vector file in pieces when writing to S3. For instance, if borders is "DEPARTEMENT", filter_by can be "REGION" or "FRANCE_ENTIERE". Default is "region".
    year (typing.Union[str, int, float]): The year of the vector file. Default is "2022".
    value (typing.Union[str, int, float]): The value of the vector file. Default is "28".
    crs (typing.Union[list, str, int, float]): The coordinate reference system of the vector file. Default is 2154.

    Returns:
    str: The URL of the vector file stored in S3
    """

    path_within = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=provider,
        source=source,
        vectorfile_format=vectorfile_format,
        borders=borders,
        filter_by=filter_by,
        year=year,
        crs=crs,
        value=value,
    )

    url = f"{ENDPOINT_URL}/{path_within}"

    print(url)

    return url


def create_path_bucket(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    vectorfile_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = "2022",
    value: typing.Union[str, int, float] = "28",
    crs: typing.Union[str, int, float] = 2154,
) -> str:
    """
    This function creates a file path for a vector file within a specified bucket.

    Parameters:
    bucket (str): The name of the bucket where the file will be stored.
    path_within_bucket (str): The path within the bucket where the file will be stored.
    vectorfile_format (str): The format of the vector file,
        can be "geojson", "topojson", "gpkg" or "shp". Default is "geojson".
    borders (str): The administrative borders of the tiles within the vector file.
        Can be any administrative
        borders provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    filter_by (str): The administrative borders (supra to 'borders') that will be
        used to cut the vector file in pieces when writing to S3. For instance, if
        borders is "DEPARTEMENT", filter_by can be "REGION" or "FRANCE_ENTIERE".
        Default is "region".
    year (str): The year of the vector file. Default is "2022".
    value (str): The value of the vector file. Default is "28".
    crs (int): The coordinate reference system of the vector file. Default is 2154.

    Returns:
    str: The complete file path for the vector file that will be used to read
    or write when interacting with S3 storage
    """

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


