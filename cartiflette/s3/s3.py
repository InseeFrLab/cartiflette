"""Module for communication with Minio S3 Storage
"""

import os
import tempfile
import s3fs
import geopandas as gpd

from cartiflette.utils import (
    keep_subset_geopandas,
    dict_corresp_decoupage,
    create_format_standardized,
    create_format_driver,
)
from cartiflette.download import get_shapefile_ign

BUCKET = "lgaliana"
PATH_WITHIN_BUCKET = "cartogether/shapefiles-test"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})


def create_path_bucket(
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
    shapefile_format="geojson",
    decoupage="region",
    year="2022",
    value="28",
):

    write_path = f"{bucket}/{path_within_bucket}/{year}"
    write_path = f"/{decoupage}/{value}/{shapefile_format}"
    write_path = f"/raw.{shapefile_format}"

    if shapefile_format == "shp":
        write_path = write_path.rsplit("/", maxsplit=1)[0] + "/"
    return write_path


def download_shapefile_s3_single(
    value="28",
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):
    # corresp_decoupage_columns = dict_corresp_decoupage()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_read = format_standardized[shapefile_format.lower()]
    driver = gpd_driver[format_read]

    read_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        shapefile_format=format_read,
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
            fs.download(remote_file, f"{tdir.name}/{remote_file.replace(dir_s3, '')}")
        object = gpd.read_file(f"{tdir.name}/raw.shp", driver=None)
    elif format_read == "parquet":
        with fs.open(read_path, "rb") as f:
            object = gpd.read_parquet(f)
    else:
        with fs.open(read_path, "rb") as f:
            object = gpd.read_file(f, driver=driver)

    return object


def write_shapefile_subset(
    object,
    value="28",
    shapefile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):

    corresp_decoupage_columns = dict_corresp_decoupage()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_write = format_standardized[shapefile_format.lower()]
    driver = gpd_driver[format_write]

    write_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        shapefile_format=format_write,
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
        write_shapefile_s3_shp(
            object=object_subset, fs=fs, write_path=write_path, driver=driver
        )
    elif format_write == "parquet":
        with fs.open(write_path, "wb") as f:
            object_subset.to_parquet(f)
    else:
        with fs.open(write_path, "wb") as f:
            object_subset.to_file(f, driver=driver)


def write_shapefile_all_levels(
    object,
    level_var,
    shapefile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):

    [
        write_shapefile_subset(
            object,
            shapefile_format=shapefile_format,
            decoupage=decoupage,
            year=year,
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            value=level,
        )
        for level in object[level_var].unique()
    ]


def write_shapefile_s3_shp(object, fs, write_path, driver=None):

    print("When using shp format, we first need a local temporary save")

    tdir = tempfile.TemporaryDirectory()
    object.to_file(tdir.name + "/raw.shp", driver=driver)

    list_files_shp = os.listdir(tdir.name)

    [
        fs.put(f"{tdir.name}/{file_name}", f"{write_path}{file_name}")
        for file_name in list_files_shp
    ]


def write_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):

    corresp_decoupage_columns = dict_corresp_decoupage()
    var_decoupage = corresp_decoupage_columns[decoupage]

    # IMPORT SHAPEFILES ------------------

    territories = {
        f: get_shapefile_ign(level=level, year=year, field=f)
        for f in ["metropole", "martinique", "reunion", "guadeloupe", "guyane"]
    }

    # WRITE ALL

    for territory in territories:
        print(f"Writing {territory}")
        write_shapefile_all_levels(
            object=territories[territory],
            level_var=var_decoupage,
            shapefile_format=shapefile_format,
            decoupage=decoupage,
            year=year,
        )


def open_shapefile_from_s3(shapefile_format, decoupage, year, value):
    read_path = create_path_bucket(
        shapefile_format=shapefile_format, decoupage=decoupage, year=year, value=value
    )
    return fs.open(read_path, mode="r")


def write_shapefile_from_s3(
    filename: str,
    decoupage: str,
    year: int,
    value: str,
    shapefile_format: str = "geojson",
):
    """Retrieve shapefiles stored in S3

    Args:
        filename (str): Filename
        decoupage (str): _description_
        year (int): Year that should be used
        value (str): Which value should be retrieved
        shapefile_format (str, optional): Shapefile format needed. Defaults to "geojson".
    """

    read_path = create_path_bucket(
        shapefile_format=shapefile_format, decoupage=decoupage, year=year, value=value
    )

    fs.download(read_path, filename)

    print(f"Requested file has been saved at location {filename}")
