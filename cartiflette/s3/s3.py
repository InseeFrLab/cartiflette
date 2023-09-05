"""Module for communication with Minio S3 Storage
"""

from datetime import date
import geopandas as gpd
import itertools
import logging
import os
import pandas as pd
import s3fs
import tempfile
from topojson import Topology
import typing


from cartiflette.download import MasterScraper, Dataset

from cartiflette.utils import (
    keep_subset_geopandas,
    dict_corresp_filter_by,
    official_epsg_codes,
    _vectorfile_format_config,
    _vectorfile_path,
)

from cartiflette.download import (
    get_vectorfile_communes_arrondissement,
    get_cog_year,
)
import cartiflette

logger = logging.getLogger(__name__)

# UTILITIES --------------------------------


def create_dict_all_territories(
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    year: int = None,
    borders: str = "COMMUNE",
) -> dict:
    # A déplacer + intégrer COG / scrapper ?
    territories_available = [
        "metropole",
        "martinique",
        "reunion",
        "guadeloupe",
        "guyane",
    ]
    if not year:
        year = date.today().year

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


# UPLOAD S3 -------------------------------


def write_cog_s3(
    year: int = None,
    vectorfile_format="json",
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : docstring
    if not year:
        year = date.today().year
    list_cog = get_cog_year(year)

    dict_path_data = {
        _vectorfile_path(
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            provider="INSEE",
            source="COG",
            vectorfile_format=vectorfile_format,
            borders=level,
            filter_by="france_entiere",
            year=year,
            value=value,
            crs=None,
            type_url="bucket",
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
    year: int = None,
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
        The year of the vector file to be written, by default the current year.
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
    if not year:
        year = date.today().year

    corresp_filter_by_columns = dict_corresp_filter_by()
    format_write, driver = _vectorfile_format_config(vectorfile_format)

    write_path = _vectorfile_path(
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
        type_url="bucket",
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
    years=[],
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
        years (list): List of years to duplicate vector files from. Default is [date.today().year].
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
    if not years:
        years = [date.today().year]

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


def write_vectorfile_all_borders(
    object: gpd.GeoDataFrame,
    borders_var: str,
    borders: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = None,
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
        year (typing.Union[str, int, float], optional): The year of the vector file. Defaults to current year.
        bucket (str, optional): The S3 bucket where to write the vector file. Defaults to BUCKET.
        path_within_bucket (str, optional): The path within the bucket where to write the vector file. Defaults to PATH_WITHIN_BUCKET.
        crs (typing.Union[str, int, float], optional): The Coordinate Reference System of the vector file. Defaults to 2154.
    """
    if not year:
        year = date.today().year

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
    year: int = None,
    filter_by="region",
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs=2154,
    borders=None,  # used to ensure we produce for "metropole" only
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    if not year:
        year = date.today().year

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
    year=None,
    crs: int = None,
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    provider="IGN",
    source="EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = cartiflette.FS,
):
    # TODO : docstring
    if not year:
        year = date.today().year
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
    fs: s3fs.S3FileSystem = cartiflette.FS,
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
):
    # TODO : docstring
    read_path = _vectorfile_path(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider="IGN",
        source="EXPRESS-COG-TERRITOIRE",
        vectorfile_format=vectorfile_format,
        borders="COMMUNE",
        filter_by=filter_by,
        year=year,
        value=value,
        crs=crs,
        type_url="bucket",
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
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
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

    read_path = _vectorfile_path(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=provider,
        source=source,
        vectorfile_format=vectorfile_format,
        borders="COMMUNE",
        filter_by=filter_by,
        year=year,
        value=value,
        crs=crs,
        type_url="bucket",
    )

    fs.download(read_path, filename)

    logger.info(f"Requested file has been saved at location {filename}")


def create_territories(
    borders: str = "COMMUNE",
    filter_by: str = "region",
    vectorfile_format: str = "geojson",
    year: int = None,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs: int = None,
):
    # TODO : docstring
    if not year:
        year = date.today().year
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
    df = gpd.pd.DataFrame(written_borderss, columns=["paths"])

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
