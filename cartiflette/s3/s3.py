"""Module for communication with Minio S3 Storage
"""

import os
import tempfile
import typing
import s3fs
import pandas as pd
import geopandas as gpd

from cartiflette.utils import (
    keep_subset_geopandas,
    dict_corresp_decoupage,
    create_format_standardized,
    create_format_driver,
)
from cartiflette.download import get_vectorfile_ign

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "diffusion/shapefiles-test"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})

def create_url_s3(
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
    vectorfile_format="geojson",
    level="COMMUNE",
    decoupage="region",
    year="2022",
    value="28"    
):

    path_within = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        vectorfile_format=vectorfile_format,
        level=level,
        decoupage=decoupage,
        year=year,
        value=value    
    )
    return f"{ENDPOINT_URL}/{path_within}"


def create_path_bucket(
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
    vectorfile_format="geojson",
    level="COMMUNE",
    decoupage="region",
    year="2022",
    value="28",
):

    write_path = f"{bucket}/{path_within_bucket}/{year}"
    write_path = f"{write_path}/{level}"
    write_path = f"{write_path}/{decoupage}/{value}/{vectorfile_format}"
    write_path = f"{write_path}/raw.{vectorfile_format}"

    if vectorfile_format == "shp":
        write_path = write_path.rsplit("/", maxsplit=1)[0] + "/"
    return write_path

def download_vectorfile_s3_all(
    values: typing.Union[list, str, int, float] = "28",
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022):

    if isinstance(values, (str, int, float)):
        values = [str(values)]

    vectors = [
        download_vectorfile_s3_single(
            value=val,
            level=level,
            vectorfile_format=vectorfile_format,
            decoupage=decoupage,
            year=year) for val in values
    ]

    vectors = pd.concat(vectors)

    return vectors


def download_vectorfile_url_all(
    values: typing.Union[list, str, int, float] = "28",
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022):

    if isinstance(values, (str, int, float)):
        values = [str(values)]

    vectors = [
        download_vectorfile_url_single(
            value=val,
            level=level,
            vectorfile_format=vectorfile_format,
            decoupage=decoupage,
            year=year) for val in values
    ]

    vectors = pd.concat(vectors)

    return vectors


def download_vectorfile_s3_single(
    value="28",
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):
    # corresp_decoupage_columns = dict_corresp_decoupage()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_read = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_read]

    read_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        vectorfile_format=format_read,
        level=level,
        decoupage=decoupage,
        year=year,
        value=value,
    )

    try:
        fs.exists(read_path)
    except:
        raise Exception("Shapefile has not been found")

    if format_read == "shp":
        dir_s3 = read_path
        print("When using shp format, we first need to store a local version")
        tdir = tempfile.TemporaryDirectory()
        for remote_file in fs.ls(dir_s3):
            fs.download(
                remote_file,
                f"{tdir.name}/{remote_file.replace(dir_s3, '')}"
                )
        object = gpd.read_file(
            f"{tdir.name}/raw.shp", driver=None
            )
    elif format_read == "parquet":
        with fs.open(read_path, "rb") as f:
            object = gpd.read_parquet(f)
    else:
        with fs.open(read_path, "rb") as f:
            object = gpd.read_file(
                f, driver=driver
                )

    return object

def download_vectorfile_url_single(
    value="28",
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET
):

    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_read = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_read]

    url = create_url_s3(
        value=value,
        level=level,
        vectorfile_format=vectorfile_format,
        decoupage=decoupage,
        year=year,
        bucket=bucket,
        path_within_bucket=path_within_bucket
    )

    if format_read == "shp":
        print("Not yet implemented")
    elif format_read == "parquet":
        object = gpd.read_parquet(url)
    else:
        object = gpd.read_file(
            url, driver=driver
            )

    return object


def write_vectorfile_subset(
    object,
    value="28",
    vectorfile_format="geojson",
    level="COMMUNE",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):

    corresp_decoupage_columns = dict_corresp_decoupage()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_write = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_write]

    write_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        vectorfile_format=format_write,
        level=level,
        decoupage=decoupage,
        year=year,
        value=value,
    )

    if fs.exists(write_path):
        if format_write == "shp":
            dir_s3 = write_path
            [fs.rm(path_s3) for path_s3 in fs.ls(dir_s3)]
        else:
            fs.rm(write_path)  # single file

    object_subset = keep_subset_geopandas(
        object, corresp_decoupage_columns[decoupage], value
    )

    if format_write == "shp":
        write_vectorfile_s3_shp(
            object=object_subset, fs=fs, write_path=write_path, driver=driver
        )
    elif format_write == "parquet":
        with fs.open(write_path, "wb") as f:
            object_subset.to_parquet(f)
    else:
        with fs.open(write_path, "wb") as f:
            object_subset.to_file(f, driver=driver)


def write_vectorfile_all_levels(
    object,
    level_var,
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):

    [
        write_vectorfile_subset(
            object,
            vectorfile_format=vectorfile_format,
            decoupage=decoupage,
            year=year,
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            value=obs,
            level=level
        )
        for obs in object[level_var].unique()
    ]


def write_vectorfile_s3_shp(object, fs, write_path, driver=None):

    print("When using shp format, we first need a local temporary save")

    tdir = tempfile.TemporaryDirectory()
    object.to_file(tdir.name + "/raw.shp", driver=driver)

    list_files_shp = os.listdir(tdir.name)

    [
        fs.put(f"{tdir.name}/{file_name}", f"{write_path}{file_name}")
        for file_name in list_files_shp
    ]


def write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):

    corresp_decoupage_columns = dict_corresp_decoupage()
    var_decoupage = corresp_decoupage_columns[decoupage]

    # IMPORT SHAPEFILES ------------------

    territories_available = [
        "metropole", "martinique",
        "reunion", "guadeloupe", "guyane"
        ]
    
    if level == "ARRONDISSEMENT_MUNICIPAL":
        territories_available = [territories_available[0]]

    territories = {
        f: get_vectorfile_ign(level=level, year=year, field=f)
        for f in territories_available
    }

    # WRITE ALL

    for territory in territories:
        print(f"Writing {territory}")
        write_vectorfile_all_levels(
            object=territories[territory],
            level=level,
            level_var=var_decoupage,
            vectorfile_format=vectorfile_format,
            decoupage=decoupage,
            year=year,
        )


def open_vectorfile_from_s3(vectorfile_format, decoupage, year, value):
    read_path = create_path_bucket(
        vectorfile_format=vectorfile_format, decoupage=decoupage, year=year, value=value
    )
    return fs.open(read_path, mode="r")


def write_vectorfile_from_s3(
    filename: str,
    decoupage: str,
    year: int,
    value: str,
    vectorfile_format: str = "geojson",
):
    """Retrieve shapefiles stored in S3

    Args:
        filename (str): Filename
        decoupage (str): _description_
        year (int): Year that should be used
        value (str): Which value should be retrieved
        vectorfile_format (str, optional): vectorfile format needed. Defaults to "geojson".
    """

    read_path = create_path_bucket(
        vectorfile_format=vectorfile_format,
        decoupage=decoupage,
        year=year,
        value=value
    )

    fs.download(read_path, filename)

    print(f"Requested file has been saved at location {filename}")
