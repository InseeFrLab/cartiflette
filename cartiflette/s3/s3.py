"""Module for communication with Minio S3 Storage
"""

import itertools
from collections import ChainMap
import os
import tempfile
import logging
import typing
import s3fs
import pandas as pd
import geopandas as gpd
from topojson import Topology

from cartiflette.download import MasterScraper, Dataset

from cartiflette.utils import (
    keep_subset_geopandas,
    dict_corresp_filter_by,
    create_format_standardized,
    create_format_driver,
    download_pb,
    official_epsg_codes,
)

from cartiflette.download import (
    # store_vectorfile_ign,
    get_vectorfile_ign,
    get_vectorfile_communes_arrondissement,
    get_cog_year,
)
import cartiflette

logger = logging.getLogger(__name__)

# UTILITIES --------------------------------

# Plus utilisé ?
# def structure_path_raw_ign(c):
#     source, field, year, provider = c
#     path = store_vectorfile_ign(
#         source=source, year=year, field=field, provider=provider
#     )
#     return {f"{year=}/raw/{provider=}/{source=}/{field=}": path}


def standardize_inputs(vectorfile_format: str) -> tuple:
    # TODO (docstring)
    corresp_filter_by_columns = dict_corresp_filter_by()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_write = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_write]

    return corresp_filter_by_columns, format_write, driver


def create_dict_all_territories(
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    year: int = 2022,
    borders: str = "COMMUNE",
) -> dict:
    territories_available = [
        "metropole",
        "martinique",
        "reunion",
        "guadeloupe",
        "guyane",
    ]

    if borders == "ARRONDISSEMENT_MUNICIPAL":
        territories_available = [territories_available[0]]

    territories = {
        f: get_vectorfile_ign(
            provider=provider,
            borders=borders,
            year=year,
            field=f,
            source=source,
        )
        for f in territories_available
    }

    return territories


# CREATE STANDARDIZED PATHS ------------------------


def create_url_s3(
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
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

    url = f"{cartiflette.ENDPOINT_URL}/{path_within}"

    return url


def create_path_bucket(
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
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


# DOWNLOAD FROM S3 --------------------------


def download_vectorfile_s3_all(
    values: typing.Union[list, str, int, float] = "28",
    borders: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = 2022,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = cartiflette.FS,
) -> gpd.GeoDataFrame:
    """
    This function downloads multiple vector files from a specified S3 bucket and returns them as a GeoPandas object.

    Parameters:
    values (list or str or int or float): The values of the vector files. Default is "28".
    borders (str): The administrative borders of the tiles within the vector file.
         Can be any administrative borders provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    vectorfile_format (str):
         The format of the vector file, can be "geojson", "topojson", "gpkg" or "shp". Default is "geojson".
    filter_by (str): The administrative borders (supra to 'borders') that will be used to cut the
          vector file in pieces when writing to S3.
          For instance, if borders is "DEPARTEMENT", filter_by can be "REGION" or "FRANCE_ENTIERE". Default is "region".
    year (int or float): The year of the vector file. Default is 2022
    provider (str) : dataset provider (from the yaml config file). The default is "IGN"
    source (str) : dataset source (from the yaml config file). The default is "EXPRESS-COG-TERRITOIRE"
    fs (s3fs.S3FileSystem): file system (s3). The default is cartiflette.FS

    Returns:
    gpd.GeoDataFrame: The vector files as a GeoPandas object
    """

    if isinstance(values, (str, int, float)):
        values = [str(values)]

    vectors = [
        download_vectorfile_s3_single(
            value=val,
            borders=borders,
            vectorfile_format=vectorfile_format,
            filter_by=filter_by,
            year=year,
            provider=provider,
            source=source,
            fs=fs,
        )
        for val in values
    ]

    # TODO : contrôler le type : pandas ou geopandas ?
    vectors = pd.concat(vectors)

    return vectors


def download_vectorfile_url_all(
    values: typing.Union[str, int, float] = "28",
    borders: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    filter_by: str = "region",
    year: int = 2022,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs: str = None,
) -> gpd.GeoDataFrame:
    # TODO : docstring + contrôler type de sortie

    if isinstance(values, (str, int, float)):
        values = [str(values)]

    vectors = [
        download_vectorfile_url_single(
            value=val,
            borders=borders,
            vectorfile_format=vectorfile_format,
            filter_by=filter_by,
            year=year,
            provider=provider,
            source=source,
            crs=crs,
        )
        for val in values
    ]

    vectors = pd.concat(vectors)

    return vectors


def download_vectorfile_s3_single(
    value: str = "28",
    borders: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = 2022,
    crs: typing.Union[str, int, float] = 2154,
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = cartiflette.FS,
) -> gpd.GeoDataFrame:
    """
    This function downloads a vector file from a specified S3 bucket and returns it

    Parameters:
    value (str): The value of the vector file. Default is "28".
    borders (str): The administrative borders of the tiles within the vector file. Can be any administrative borders provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    vectorfile_format (str): The format of the vector file, can be "geojson", "topojson", "gpkg" or "shp". Default is "geojson".
    filter_by (str): The administrative borders (supra to 'borders') that will be used to cut the vector file in pieces when writing to S3. For instance, if borders is "DEPARTEMENT", filter_by can be "REGION" or "FRANCE_ENTIERE". Default is "region".
    year (int): The year of the vector file. Default is 2022
    crs (int): The coordinate reference system of the vector file. Default is 2154.
    bucket (str): The name of the bucket where the file will be stored. Default is BUCKET
    path_within_bucket (str): The path within the bucket where the file will be stored. Default is PATH_WITHIN_BUCKET
    provider (str) : dataset provider (from the yaml config file). The default is "IGN"
    source (str) : dataset source (from the yaml config file). The default is "EXPRESS-COG-TERRITOIRE"
    fs (s3fs.S3FileSystem): file system (s3). The default is cartiflette.FS


    Returns:
    GeoPandas.GeoDataFrame: The vector file as a GeoPandas object

    """

    corresp_filter_by_columns, format_read, driver = standardize_inputs(
        vectorfile_format
    )

    read_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        vectorfile_format=format_read,
        borders=borders,
        filter_by=filter_by,
        year=year,
        value=value,
        crs=crs,
        provider=provider,
        source=source,
    )

    try:
        fs.exists(read_path)
    except:
        raise Exception("Shapefile has not been found")

    if format_read == "shp":
        dir_s3 = read_path
        logger.info(
            "When using shp format, we first need to store a local version"
        )
        tdir = tempfile.TemporaryDirectory()
        for remote_file in fs.ls(dir_s3):
            fs.download(
                remote_file, f"{tdir.name}/{remote_file.replace(dir_s3, '')}"
            )
        object = gpd.read_file(f"{tdir.name}/raw.shp", driver=None)
    elif format_read == "parquet":
        with fs.open(read_path, "rb") as f:
            object = gpd.read_parquet(f)
    else:
        with fs.open(read_path, "rb") as f:
            object = gpd.read_file(f, driver=driver)

    return object


def download_vectorfile_url_single(
    value: str = "28",
    borders: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = 2022,
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs=None,
):
    """
    This function downloads a vector file from a specified S3 bucket and returns it as a GeoPandas object.

    Parameters:
    value (str or int): The value of the vector file. Default is "28".
    borders (str): The administrative borders of the tiles within the vector file.
        Can be any administrative borders provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    vectorfile_format (str): The format of the vector file,
        can be "geojson", "topojson", "gpkg" or "shp". Default is "geojson".
    filter_by (str): The administrative borders (supra to 'borders') that will be used to cut the vector file in pieces when writing to S3.
        For instance, if borders is "DEPARTEMENT", filter_by can be "REGION" or "FRANCE_ENTIERE". Default is "region".
    year (int or float): The year of the vector file. Default is 2022.
    bucket (str): The name of the bucket where the file is stored. Default is inherited from package configuration.
    path_within_bucket (str): The path within the bucket where the file is stored. Default is PATH_WITHIN_BUCKET.
    provider (str) : dataset provider (from the yaml config file). The default is "IGN"
    source (str) : dataset source (from the yaml config file). The default is "EXPRESS-COG-TERRITOIRE"
    crs (int): The coordinate reference system of the vector file. Default is None.

    Returns:
    gpd.GeoDataFrame: The vector file as a GeoPandas object
    """

    corresp_filter_by_columns, format_read, driver = standardize_inputs(
        vectorfile_format
    )

    url = create_url_s3(
        value=value,
        borders=borders,
        vectorfile_format=format_read,
        filter_by=filter_by,
        year=year,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=provider,
        source=source,
        crs=crs,
    )

    if format_read == "shp":
        logger.warning("Not yet implemented")
    elif format_read == "parquet":
        tmp = tempfile.NamedTemporaryFile(delete=False)
        download_pb(url, tmp.name)
        object = gpd.read_parquet(tmp.name)
    else:
        tmp = tempfile.NamedTemporaryFile(delete=False)
        download_pb(url, tmp.name)
        object = gpd.read_file(url, driver=driver)

    if format_read == "topojson":
        object.crs = crs

    return object


# UPLOAD S3 -------------------------------


def write_cog_s3(
    year: int = 2022,
    vectorfile_format="json",
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : docstring
    list_cog = get_cog_year(year)

    dict_path_data = {
        create_path_bucket(
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            provider="INSEE",
            source="COG",
            vectorfile_format=vectorfile_format,
            borders=level,
            filter_by="france_entiere",
            year=year,
            value="raw",
            crs=None,
        ): value
        for level, value in list_cog.items()
    }

    for path, data in dict_path_data.items():
        with fs.open(path, "wb") as f:
            if vectorfile_format == "json":
                data.to_json(f, orient="records")
            elif vectorfile_format == "parquet":
                data.to_parquet(f)
            elif vectorfile_format == "csv":
                data.to_csv(f)
            else:
                logger.error("Unsupported format")


def write_vectorfile_subset(
    object: gpd.GeoDataFrame,
    value: str = "28",
    vectorfile_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    year: int = 2022,
    crs: typing.Union[str, int, float] = 2154,
    force_crs: bool = False,
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = cartiflette.FS,
) -> None:
    """
    This function writes a subset of a given vector file to a specified bucket in S3.

    Parameters
    ----------
    object : gpd.GeoDataFrame
        The input vector file as a GeoPandas DataFrame.
    value : str, optional
        The value of the subset of the vector file to be written, by default "28".
    vectorfile_format : str, optional
        The format of the vector file to be written, by default "geojson".
    borders : str, optional
        The borders of the vector file to be written, by default "COMMUNE".
    filter_by : str, optional
        The filter_by of the vector file to be written, by default "region".
    year : int, optional
        The year of the vector file to be written, by default 2022.
    bucket : str, optional
        The S3 bucket where the vector file will be written, by default BUCKET.
    path_within_bucket : str, optional
        The path within the specified S3 bucket where the vector file will be written, by default PATH_WITHIN_BUCKET.
    crs : typing.Union[str, int, float], optional
        The coordinate reference system to be used, by default 2154.
    fs : s3fs.S3FileSystem, optional
        File system (s3). The default is cartiflette.FS

    Returns
    -------
    None
    """

    corresp_filter_by_columns, format_write, driver = standardize_inputs(
        vectorfile_format
    )

    write_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=provider,
        source=source,
        vectorfile_format=format_write,
        borders=borders,
        filter_by=filter_by,
        year=year,
        value=value,
        crs=crs,
    )

    logger.info(f"Writing file at {write_path} location")

    if fs.exists(write_path):
        if format_write == "shp":
            dir_s3 = write_path
            [fs.rm(path_s3) for path_s3 in fs.ls(dir_s3)]
        else:
            fs.rm(write_path)  # single file

    object_subset = keep_subset_geopandas(
        object, corresp_filter_by_columns[filter_by.lower()], value
    )

    if format_write.lower() in ["geojson", "topojson"]:
        if crs != 4326:
            logger.warning(
                "geojson are supposed to adopt EPSG 4326\
                Forcing the projection used"
            )
            if force_crs is False:
                return None
            crs = 4326

    if object_subset.crs != crs:
        object_subset = object_subset.to_crs(crs)

    if format_write == "shp":
        write_vectorfile_s3_shp(
            object=object_subset, fs=fs, write_path=write_path, driver=driver
        )
    elif format_write == "parquet":
        with fs.open(write_path, "wb") as f:
            object_subset.to_parquet(f)
    elif format_write == "topojson":
        tdir = tempfile.TemporaryDirectory()
        object_topo = Topology(object_subset)
        object_topo.to_json(tdir.name + "temp.json")
        fs.put(tdir.name + "temp.json", write_path)
    else:
        with fs.open(write_path, "wb") as f:
            object_subset.to_file(f, driver=driver)


def duplicate_vectorfile_ign(
    providers=["IGN"],
    dataset_family=["ADMINEXPRESS"],
    sources=["EXPRESS-COG-TERRITOIRE"],
    territories=["guyane"],
    years=[2022],
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = cartiflette.FS,
    base_cache_pattern: str = cartiflette.BASE_CACHE_PATTERN,
):
    # TODO : compléter la docstring
    """Duplicate and store vector files from IGN dataset.

    This function duplicates vector files from the IGN (Institut national de l'information géographique et forestière)
    dataset and stores them in a specified bucket on an S3-compatible storage system. It also maintains a record of the
    duplicated files' metadata, such as dataset family, source, year, provider, territory, normalized path in the bucket,
    and the MD5 hash of the file.

    Args:
        providers (list): List of provider names to duplicate vector files from. Default is ['IGN'].
        dataset_family (list): List of dataset family names to duplicate vector files from. Default is ['ADMINEXPRESS'].
        sources (list): List of source names to duplicate vector files from. Default is ['EXPRESS-COG-TERRITOIRE'].
        territories (list): List of territory names to duplicate vector files from. Default is ['guyane'].
        years (list): List of years to duplicate vector files from. Default is [2022].
        bucket (str): Name of the S3-compatible bucket where the files will be stored.
        path_within_bucket (str): Path within the bucket to store the duplicated files.

    Returns:
        None

    Raises:
        None

    Example Usage:
        duplicate_vectorfile_ign(
            providers=['IGN', 'AnotherProvider'],
            dataset_family=['ADMINEXPRESS', 'AnotherFamily'],
            sources=['EXPRESS-COG-TERRITOIRE', 'AnotherSource'],
            territories=['guyane', 'AnotherTerritory'],
            years=[2022, 2023],
            bucket='my-s3-bucket',
            path_within_bucket='data/ign'
        )
    """

    combinations = list(
        itertools.product(
            sources, territories, years, providers, dataset_family
        )
    )

    with MasterScraper() as s:
        for source, territory, year, provider, dataset_family in combinations:
            datafile = Dataset(
                dataset_family,
                source,
                year,
                provider,
                territory,
                bucket,
                path_within_bucket,
                fs,
            )

            result = s.download_unzip(
                datafile,
                preserve="shape",
                pattern=base_cache_pattern,
                ext=".shp",
            )

            if not result["downloaded"]:
                logger.info("File already there and uptodate")
                return

            # DUPLICATE SOURCE IN BUCKET
            normalized_path_bucket = (
                f"{year=}/raw/{provider=}/{source=}/{territory=}"
            )
            normalized_path_bucket = normalized_path_bucket.replace("'", "")
            normalized_path = {normalized_path_bucket: result["path"][0]}

            for path_s3fs, path_local_fs in normalized_path.items():
                logger.info(f"Iterating over {path_s3fs}")
                fs.put(
                    path_local_fs,
                    f"{bucket}/{path_within_bucket}/{path_s3fs}",
                    recursive=True,
                )

            # NOW WRITE MD5 IN BUCKET ROOT
            datafile.update_json_md5(result["hash"])


# Plus utilisé ?
# def duplicate_vectorfile_ign_old(
#     sources: list,
#     territories: list,
#     years: list,
#     providers: list,
#     bucket=cartiflette.BUCKET,
#     path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
#     endpoint_url=cartiflette.ENDPOINT_URL,
# ):
#     """
#     Duplicates a list of vector files to a specified Amazon S3 bucket using s3fs.

#     Args:
#     - sources (list): A list of source names (strings) to combine with other parameters to form file paths.
#     - territories (list): A list of territory names (strings) to combine with other parameters to form file paths.
#     - years (list): A list of year values (strings or integers) to combine with other parameters to form file paths.
#     - providers (list): A list of provider names (strings) to combine with other parameters to form file paths.
#     - BUCKET (string): The name of the Amazon S3 bucket to write the duplicated files to (default: "projet-cartiflette").
#     - PATH_WITHIN_BUCKET (string): The prefix within the bucket to write the duplicated files to (default: "diffusion/shapefiles-test1").
#     - ENDPOINT_URL (string): The endpoint URL of the S3-compatible object storage service (default: "https://minio.lab.sspcloud.fr").

#     Returns:
#     - None: The function has no explicit return value.

#     Raises:
#     - None: The function does not raise any exceptions explicitly.
#     """

#     combinations = list(
#         itertools.product(sources, territories, years, providers)
#     )

#     paths = dict(ChainMap(*[structure_path_raw_ign(c) for c in combinations]))

#     fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint_url})

#     for path_s3fs, path_local_fs in paths.items():
#         logger.info(f"Iterating over {path_s3fs}")
#         fs.put(
#             path_local_fs,
#             f"{bucket}/{path_within_bucket}/{path_s3fs}",
#             recursive=True,
#         )


def write_vectorfile_all_borders(
    object: gpd.GeoDataFrame,
    borders_var: str,
    borders: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = 2022,
    crs: typing.Union[str, int, float] = 2154,
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : compléter la docstring
    """Write all borderss of a GeoDataFrame to a specified format on S3.

    This function takes a GeoDataFrame object, the variable name on which to create
    the borderss and other parameters like format, filter_by, year, bucket and path
    within the bucket, crs and borders for the vector file to be written on S3.

    Args:
        object (gpd.GeoDataFrame): The GeoDataFrame object to write.
        borders_var (str): The variable name on which to create the borderss.
        borders (str, optional): The borders of the vector file. Defaults to "COMMUNE".
        vectorfile_format (str, optional): The format of the vector file. Defaults to "geojson".
        filter_by (str, optional): The filter_by of the vector file. Defaults to "region".
        year (typing.Union[str, int, float], optional): The year of the vector file. Defaults to 2022.
        bucket (str, optional): The S3 bucket where to write the vector file. Defaults to BUCKET.
        path_within_bucket (str, optional): The path within the bucket where to write the vector file. Defaults to PATH_WITHIN_BUCKET.
        crs (typing.Union[str, int, float], optional): The Coordinate Reference System of the vector file. Defaults to 2154.
    """

    [
        write_vectorfile_subset(
            object,
            vectorfile_format=vectorfile_format,
            filter_by=filter_by,
            year=year,
            value=obs,
            borders=borders,
            crs=crs,
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            provider=provider,
            source=source,
            fs=fs,
        )
        for obs in object[borders_var].unique()
    ]


def write_vectorfile_s3_shp(object, fs, write_path, driver=None):
    # TODO : docstring
    logger.info("When using shp format, we first need a local temporary save")

    tdir = tempfile.TemporaryDirectory()
    object.to_file(tdir.name + "/raw.shp", driver=driver)

    list_files_shp = os.listdir(tdir.name)

    [
        fs.put(f"{tdir.name}/{file_name}", f"{write_path}{file_name}")
        for file_name in list_files_shp
    ]


def write_vectorfile_s3_custom_arrondissement(
    vectorfile_format="geojson",
    year: int = 2022,
    filter_by="region",
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs=2154,
    borders=None,  # used to ensure we produce for "metropole" only
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : docstring
    if crs is None:
        if vectorfile_format.lower() == "geojson":
            crs = 4326
        else:
            crs = "official"

    corresp_filter_by_columns = dict_corresp_filter_by()

    var_filter_by_s3 = corresp_filter_by_columns[filter_by.lower()]
    filter_by = filter_by.upper()

    # CREATING CUSTOM

    object = get_vectorfile_communes_arrondissement(
        year=year, provider=provider, source=source
    )

    if filter_by.upper() == "FRANCE_ENTIERE":
        object["territoire"] = "metropole"

    write_vectorfile_all_borders(
        object=object,
        borders="COMMUNE_ARRONDISSEMENT",
        borders_var=var_filter_by_s3,
        vectorfile_format=vectorfile_format,
        filter_by=filter_by,
        year=year,
        crs=crs,
        provider=provider,
        source=source,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        fs=fs,
    )


# main function


def write_vectorfile_s3_all(
    borders="COMMUNE",
    vectorfile_format="geojson",
    filter_by="region",
    year=2022,
    crs: int = None,
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    provider="IGN",
    source="EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : docstring
    if crs is None:
        if vectorfile_format.lower() == "geojson":
            crs = 4326
        else:
            crs = "official"

    corresp_filter_by_columns = dict_corresp_filter_by()

    var_filter_by_s3 = corresp_filter_by_columns[filter_by.lower()]
    borders_read = borders.upper()
    filter_by = filter_by.upper()

    # IMPORT SHAPEFILES ------------------

    territories = create_dict_all_territories(
        provider=provider, source=source, year=year, borders=borders_read
    )

    # For whole France, we need to combine everything together
    # into new key "territoire"
    if filter_by.upper() == "FRANCE_ENTIERE":
        for key, val in territories.items():
            val["territoire"] = key

    for territory in territories:
        logger.info(f"Writing {territory}")

        if crs == "official":
            epsg = official_epsg_codes()[territory]
        else:
            epsg = crs

        write_vectorfile_all_borders(
            object=territories[territory],
            borders=borders,
            borders_var=var_filter_by_s3,
            vectorfile_format=vectorfile_format,
            filter_by=filter_by,
            year=year,
            crs=epsg,
            provider=provider,
            source=source,
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            fs=fs,
        )


# TODO : fonction non appelée et non mentionnée dans les fonctions publiques ?
def open_vectorfile_from_s3(
    vectorfile_format,
    filter_by,
    year,
    value,
    crs,
    fs,
):
    # TODO : docstring
    read_path = create_path_bucket(
        vectorfile_format=vectorfile_format,
        filter_by=filter_by,
        year=year,
        value=value,
        crs=crs,
    )
    return fs.open(read_path, mode="r")


# TODO : fonction non appelée et non mentionnée dans les fonctions publiques ?
def write_vectorfile_from_s3(
    filename: str,
    filter_by: str,
    year: int,
    value: str,
    vectorfile_format: str = "geojson",
    crs: int = 2154,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : compléter docstring
    """Retrieve shapefiles stored in S3

    Args:
        filename (str): Filename
        filter_by (str): _description_
        year (int): Year that should be used
        value (str): Which value should be retrieved
        vectorfile_format (str, optional): vectorfile format needed. Defaults to "geojson".
    """

    read_path = create_path_bucket(
        vectorfile_format=vectorfile_format,
        filter_by=filter_by,
        year=year,
        value=value,
        crs=crs,
        provider=provider,
        source=source,
    )

    fs.download(read_path, filename)

    logger.info(f"Requested file has been saved at location {filename}")


def create_territories(
    borders: str = "COMMUNE",
    filter_by: str = "region",
    vectorfile_format: str = "geojson",
    year: int = 2022,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs: int = None,
):
    # TODO : docstring
    if crs is None:
        if vectorfile_format.lower() == "geojson":
            crs = 4326
        else:
            crs = "official"

    corresp_filter_by_columns = dict_corresp_filter_by()

    var_filter_by_s3 = corresp_filter_by_columns[filter_by.lower()]
    borders_read = borders.upper()

    # IMPORT SHAPEFILES ------------------

    territories = create_dict_all_territories(
        provider=provider, source=source, year=year, borders=borders_read
    )

    return territories


def restructure_nested_dict_borderss(dict_with_list: dict):
    # TODO : docstring
    croisement_filter_by_borders_flat = [
        [key, inner_value]
        for key, values in dict_with_list.items()
        for inner_value in values
    ]

    return croisement_filter_by_borders_flat


def crossproduct_parameters_production(
    croisement_filter_by_borders, list_format, years, crs_list, sources
):
    # TODO : docstring
    croisement_filter_by_borders_flat = restructure_nested_dict_borderss(
        croisement_filter_by_borders
    )

    combinations = list(
        itertools.product(
            list_format,
            croisement_filter_by_borders_flat,
            years,
            crs_list,
            sources,
        )
    )

    tempdf = pd.DataFrame(
        combinations, columns=["format", "nested", "year", "crs", "source"]
    )
    tempdf["borders"] = tempdf["nested"].apply(lambda l: l[0])
    tempdf["filter_by"] = tempdf["nested"].apply(lambda l: l[1])
    tempdf.drop("nested", axis="columns", inplace=True)

    return tempdf


def list_produced_cartiflette(
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : docstring
    written_borderss = fs.glob(f"{bucket}/{path_within_bucket}/**/provider*")
    df = pd.DataFrame(written_borderss, columns=["paths"])

    df[["year", "administrative_level", "crs", "filter_by", "format"]] = df[
        "paths"
    ].str.extract(
        r"year=(\d+)/administrative_level=(\w+)/crs=(\d+)/(.*)=.*/vectorfile_format=\'(\w+)\'"
    )

    df = df.filter(
        ["year", "administrative_level", "crs", "filter_by", "format"],
        axis="columns",
    )
    df = df.drop_duplicates()

    with fs.open(f"{bucket}/{path_within_bucket}/available.json", "wb") as f:
        df.to_json(f, orient="records")


def production_cartiflette(
    croisement_filter_by_borders,
    formats,
    years,
    crs_list,
    sources,
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : docstring
    tempdf = crossproduct_parameters_production(
        croisement_filter_by_borders=croisement_filter_by_borders,
        list_format=formats,
        years=years,
        crs_list=crs_list,
        sources=sources,
    )

    for index, row in tempdf.iterrows():
        format = row["format"]
        borders = row["borders"]
        filter_by = row["filter_by"]
        year = row["year"]
        crs = row["crs"]
        source = row["source"]
        logger.info(
            80 * "==" + "\n"
            f"{borders=}\n{format=}\n"
            f"{filter_by=}\n{year=}\n"
            f"{crs=}\n"
            f"{source=}"
        )

        if borders == "COMMUNE_ARRONDISSEMENT":
            production_func = write_vectorfile_s3_custom_arrondissement
        else:
            production_func = write_vectorfile_s3_all

        production_func(
            borders=borders,
            vectorfile_format=format,
            filter_by=filter_by,
            year=year,
            crs=crs,
            provider="IGN",
            source=source,
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            fs=fs,
        )

    logger.info(80 * "-" + "\nProduction finished :)")


# Plus utilisé ?
# def create_nested_topojson(path):
#     #TODO : docstring
#     croisement_filter_by_borders = {
#         ## structure -> niveau geo: [niveau filter_by macro],
#         "REGION": ["FRANCE_ENTIERE"],
#         "DEPARTEMENT": ["FRANCE_ENTIERE"],
#     }

#     croisement_filter_by_borders_flat = [
#         [key, inner_value]
#         for key, values in croisement_filter_by_borders.items()
#         for inner_value in values
#     ]

#     list_output = {}
#     for couple in croisement_filter_by_borders_flat:
#         borders = couple[0]
#         filter_by = couple[1]
#         list_output[borders] = create_territories(
#             borders=borders, filter_by=filter_by
#         )

#     topo = Topology(
#         data=[
#             list_output["REGION"]["metropole"],
#             list_output["DEPARTEMENT"]["metropole"],
#         ],
#         object_name=["region", "departement"],
#         prequantize=False,
#     )

#     return topo
#     # topo.to_json(path)
