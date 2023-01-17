"""Module for communication with Minio S3 Storage
"""

import itertools
import os
import tempfile
import typing
import s3fs
import pandas as pd
import geopandas as gpd
from topojson import Topology

from cartiflette.utils import (
    keep_subset_geopandas,
    dict_corresp_decoupage,
    create_format_standardized,
    create_format_driver,
    download_pb,
    official_epsg_codes,
)
from cartiflette.download import (
    get_vectorfile_ign,
    get_vectorfile_communes_arrondissement,
)

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "diffusion/shapefiles-test1"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})

# UTILITIES --------------------------------


def standardize_inputs(vectorfile_format):

    corresp_decoupage_columns = dict_corresp_decoupage()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_write = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_write]

    return corresp_decoupage_columns, format_write, driver


def create_dict_all_territories(
    provider="IGN", source="EXPRESS-COG-TERRITOIRE", year=2022, level="COMMUNE"
):

    territories_available = [
        "metropole",
        "martinique",
        "reunion",
        "guadeloupe",
        "guyane",
    ]

    if level == "ARRONDISSEMENT_MUNICIPAL":
        territories_available = [territories_available[0]]

    territories = {
        f: get_vectorfile_ign(
            provider=provider, level=level, year=year, field=f, source=source
        )
        for f in territories_available
    }

    return territories


# CREATE STANDARDIZED PATHS ------------------------


def create_url_s3(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    vectorfile_format: str = "geojson",
    level: str = "COMMUNE",
    decoupage: str = "region",
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
    level (str): The administrative level of the tiles within the vector file. Can be any administrative level provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    decoupage (str): The administrative level (supra to 'level') that will be used to cut the vector file in pieces when writing to S3. For instance, if level is "DEPARTEMENT", decoupage can be "REGION" or "FRANCE_ENTIERE". Default is "region".
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
        level=level,
        decoupage=decoupage,
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
    level: str = "COMMUNE",
    decoupage: str = "region",
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
    level (str): The administrative level of the tiles within the vector file.
        Can be any administrative
        level provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    decoupage (str): The administrative level (supra to 'level') that will be
        used to cut the vector file in pieces when writing to S3. For instance, if
        level is "DEPARTEMENT", decoupage can be "REGION" or "FRANCE_ENTIERE".
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
    write_path = f"{write_path}/administrative_level={level}"
    write_path = f"{write_path}/{crs=}"
    write_path = f"{write_path}/{decoupage}={value}/{vectorfile_format=}"
    write_path = f"{write_path}/{provider=}/{source=}"
    write_path = f"{write_path}/raw.{vectorfile_format}"

    if vectorfile_format == "shp":
        write_path = write_path.rsplit("/", maxsplit=1)[0] + "/"
    return write_path


# DOWNLOAD FROM S3 --------------------------


def download_vectorfile_s3_all(
    values: typing.Union[list, str, int, float] = "28",
    level: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    decoupage: str = "region",
    year: typing.Union[str, int, float] = 2022,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
):
    """
    This function downloads multiple vector files from a specified S3 bucket and returns them as a GeoPandas object.

    Parameters:
    values (list or str or int or float): The values of the vector files. Default is "28".
    level (str): The administrative level of the tiles within the vector file.
         Can be any administrative level provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    vectorfile_format (str):
         The format of the vector file, can be "geojson", "topojson", "gpkg" or "shp". Default is "geojson".
    decoupage (str): The administrative level (supra to 'level') that will be used to cut the
          vector file in pieces when writing to S3.
          For instance, if level is "DEPARTEMENT", decoupage can be "REGION" or "FRANCE_ENTIERE". Default is "region".
    year (int or float): The year of the vector file. Default is 2022

    Returns:
    gpd.GeoDataFrame: The vector files as a GeoPandas object
    """

    if isinstance(values, (str, int, float)):
        values = [str(values)]

    vectors = [
        download_vectorfile_s3_single(
            value=val,
            level=level,
            vectorfile_format=vectorfile_format,
            decoupage=decoupage,
            year=year,
            provider=provider,
            source=source,
        )
        for val in values
    ]

    vectors = pd.concat(vectors)

    return vectors


def download_vectorfile_url_all(
    values: typing.Union[str, int, float] = "28",
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs = None
):

    if isinstance(values, (str, int, float)):
        values = [str(values)]

    vectors = [
        download_vectorfile_url_single(
            value=val,
            level=level,
            vectorfile_format=vectorfile_format,
            decoupage=decoupage,
            year=year,
            provider=provider,
            source=source,
            crs=crs
        )
        for val in values
    ]

    vectors = pd.concat(vectors)

    return vectors


def download_vectorfile_s3_single(
    value: str = "28",
    level: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    decoupage: str = "region",
    year: typing.Union[str, int, float] = 2022,
    crs: typing.Union[str, int, float] = 2154,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
):
    """
    This function downloads a vector file from a specified S3 bucket and returns it

    Parameters:
    value (str): The value of the vector file. Default is "28".
    level (str): The administrative level of the tiles within the vector file. Can be any administrative level provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    vectorfile_format (str): The format of the vector file, can be "geojson", "topojson", "gpkg" or "shp". Default is "geojson".
    decoupage (str): The administrative level (supra to 'level') that will be used to cut the vector file in pieces when writing to S3. For instance, if level is "DEPARTEMENT", decoupage can be "REGION" or "FRANCE_ENTIERE". Default is "region".
    year (int): The year of the vector file. Default is 2022
    bucket (str): The name of the bucket where the file will be stored. Default is BUCKET
    path_within_bucket (str): The path within the bucket where the file will be stored. Default is PATH_WITHIN_BUCKET
    crs (int): The coordinate reference system of the vector file. Default is 2154.

    Returns:
    GeoPandas.GeoDataFrame: The vector file as a GeoPandas object

    """

    corresp_decoupage_columns, format_read, driver = standardize_inputs(
        vectorfile_format
    )

    read_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        vectorfile_format=format_read,
        level=level,
        decoupage=decoupage,
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


def download_vectorfile_url_single(
    value: str = "28",
    level: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    decoupage: str = "region",
    year: typing.Union[str, int, float] = 2022,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs = None
):
    """
    This function downloads a vector file from a specified S3 bucket and returns it as a GeoPandas object.

    Parameters:
    value (str or int): The value of the vector file. Default is "28".
    level (str): The administrative level of the tiles within the vector file.
        Can be any administrative level provided by IGN, e.g. "COMMUNE", "DEPARTEMENT" or "REGION". Default is "COMMUNE".
    vectorfile_format (str): The format of the vector file,
        can be "geojson", "topojson", "gpkg" or "shp". Default is "geojson".
    decoupage (str): The administrative level (supra to 'level') that will be used to cut the vector file in pieces when writing to S3.
        For instance, if level is "DEPARTEMENT", decoupage can be "REGION" or "FRANCE_ENTIERE". Default is "region".
    year (int or float): The year of the vector file. Default is 2022.
    bucket (str): The name of the bucket where the file is stored. Default is inherited from package configuration.
    path_within_bucket (str): The path within the bucket where the file is stored. Default is PATH_WITHIN_BUCKET.

    Returns:
    gpd.GeoDataFrame: The vector file as a GeoPandas object
    """

    corresp_decoupage_columns, format_read, driver = standardize_inputs(
        vectorfile_format
    )

    url = create_url_s3(
        value=value,
        level=level,
        vectorfile_format=format_read,
        decoupage=decoupage,
        year=year,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=provider,
        source=source,
        crs=crs
    )

    if format_read == "shp":
        print("Not yet implemented")
    elif format_read == "parquet":
        tmp = tempfile.NamedTemporaryFile(delete=False)
        download_pb(url, tmp.name)
        object = gpd.read_parquet(tmp.name)
    else:
        tmp = tempfile.NamedTemporaryFile(delete=False)
        download_pb(url, tmp.name)
        object = gpd.read_file(url, driver=driver)

    return object


# UPLOAD S3 -------------------------------


def write_vectorfile_subset(
    object: gpd.GeoDataFrame,
    value: str = "28",
    vectorfile_format: str = "geojson",
    level: str = "COMMUNE",
    decoupage: str = "region",
    year: int = 2022,
    crs: typing.Union[str, int, float] = 2154,
    force_crs: bool = False,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
):
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
    level : str, optional
        The level of the vector file to be written, by default "COMMUNE".
    decoupage : str, optional
        The decoupage of the vector file to be written, by default "region".
    year : int, optional
        The year of the vector file to be written, by default 2022.
    bucket : str, optional
        The S3 bucket where the vector file will be written, by default BUCKET.
    path_within_bucket : str, optional
        The path within the specified S3 bucket where the vector file will be written, by default PATH_WITHIN_BUCKET.
    crs : typing.Union[str, int, float], optional
        The coordinate reference system to be used, by default 2154.

    Returns
    -------
    None
    """

    corresp_decoupage_columns, format_write, driver = standardize_inputs(
        vectorfile_format
    )

    write_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=provider,
        source=source,
        vectorfile_format=format_write,
        level=level,
        decoupage=decoupage,
        year=year,
        value=value,
        crs=crs,
    )

    print(f"Writing file at {write_path} location")

    if fs.exists(write_path):
        if format_write == "shp":
            dir_s3 = write_path
            [fs.rm(path_s3) for path_s3 in fs.ls(dir_s3)]
        else:
            fs.rm(write_path)  # single file

    object_subset = keep_subset_geopandas(
        object, corresp_decoupage_columns[decoupage.lower()], value
    )

    if format_write.lower() in ["geojson", "topojson"]:
        if crs != 4326:
            print(
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


def write_vectorfile_all_levels(
    object: gpd.GeoDataFrame,
    level_var: str,
    level: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    decoupage: str = "region",
    year: typing.Union[str, int, float] = 2022,
    crs: typing.Union[str, int, float] = 2154,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
):

    """Write all levels of a GeoDataFrame to a specified format on S3.

    This function takes a GeoDataFrame object, the variable name on which to create
    the levels and other parameters like format, decoupage, year, bucket and path
    within the bucket, crs and level for the vector file to be written on S3.

    Args:
        object (gpd.GeoDataFrame): The GeoDataFrame object to write.
        level_var (str): The variable name on which to create the levels.
        level (str, optional): The level of the vector file. Defaults to "COMMUNE".
        vectorfile_format (str, optional): The format of the vector file. Defaults to "geojson".
        decoupage (str, optional): The decoupage of the vector file. Defaults to "region".
        year (typing.Union[str, int, float], optional): The year of the vector file. Defaults to 2022.
        bucket (str, optional): The S3 bucket where to write the vector file. Defaults to BUCKET.
        path_within_bucket (str, optional): The path within the bucket where to write the vector file. Defaults to PATH_WITHIN_BUCKET.
        crs (typing.Union[str, int, float], optional): The Coordinate Reference System of the vector file. Defaults to 2154.
    """

    [
        write_vectorfile_subset(
            object,
            vectorfile_format=vectorfile_format,
            decoupage=decoupage,
            year=year,
            value=obs,
            level=level,
            crs=crs,
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            provider=provider,
            source=source,
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


def write_vectorfile_s3_custom_arrondissement(
    vectorfile_format="geojson",
    year: int = 2022,
    decoupage="region",
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs=2154,
    level=None,  # used to ensure we produce for "metropole" only
):

    if crs is None:
        if vectorfile_format.lower() == "geojson":
            crs = 4326
        else:
            crs = "official"

    corresp_decoupage_columns = dict_corresp_decoupage()

    var_decoupage_s3 = corresp_decoupage_columns[decoupage.lower()]
    decoupage = decoupage.upper()

    # CREATING CUSTOM

    object = get_vectorfile_communes_arrondissement(
        year=year, provider=provider, source=source
    )    

    if decoupage.upper() == "FRANCE_ENTIERE":
        object["territoire"] = "metropole"


    write_vectorfile_all_levels(
        object=object,
        level="COMMUNE_ARRONDISSEMENT",
        level_var=var_decoupage_s3,
        vectorfile_format=vectorfile_format,
        decoupage=decoupage,
        year=year,
        crs=crs,
        provider=provider,
        source=source,
    )


# main function


def write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    crs: int = None,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
    provider="IGN",
    source="EXPRESS-COG-TERRITOIRE",
):

    if crs is None:
        if vectorfile_format.lower() == "geojson":
            crs = 4326
        else:
            crs = "official"

    corresp_decoupage_columns = dict_corresp_decoupage()

    var_decoupage_s3 = corresp_decoupage_columns[decoupage.lower()]
    level_read = level.upper()
    decoupage = decoupage.upper()

    # IMPORT SHAPEFILES ------------------

    territories = create_dict_all_territories(
        provider=provider, source=source, year=year, level=level_read
    )

    # For whole France, we need to combine everything together
    # into new key "territoire"
    if decoupage.upper() == "FRANCE_ENTIERE":
        for key, val in territories.items():
            val["territoire"] = key

    for territory in territories:

        print(f"Writing {territory}")

        if crs == "official":
            epsg = official_epsg_codes()[territory]
        else:
            epsg = crs


        write_vectorfile_all_levels(
            object=territories[territory],
            level=level,
            level_var=var_decoupage_s3,
            vectorfile_format=vectorfile_format,
            decoupage=decoupage,
            year=year,
            crs=epsg,
            provider=provider,
            source=source,
        )


def open_vectorfile_from_s3(vectorfile_format, decoupage, year, value, crs):
    read_path = create_path_bucket(
        vectorfile_format=vectorfile_format,
        decoupage=decoupage,
        year=year,
        value=value,
        crs=crs,
    )
    return fs.open(read_path, mode="r")


def write_vectorfile_from_s3(
    filename: str,
    decoupage: str,
    year: int,
    value: str,
    vectorfile_format: str = "geojson",
    crs: int = 2154,
    provider="IGN",
    source="EXPRESS-COG-TERRITOIRE",
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
        value=value,
        crs=crs,
        provider=provider,
        source=source,
    )

    fs.download(read_path, filename)

    print(f"Requested file has been saved at location {filename}")


def create_territories(
    level: str = "COMMUNE",
    decoupage: str = "region",
    vectorfile_format: str = "geojson",
    year: int = 2022,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs: int = None,
):

    if crs is None:
        if vectorfile_format.lower() == "geojson":
            crs = 4326
        else:
            crs = "official"

    corresp_decoupage_columns = dict_corresp_decoupage()

    var_decoupage_s3 = corresp_decoupage_columns[decoupage.lower()]
    level_read = level.upper()

    # IMPORT SHAPEFILES ------------------

    territories = create_dict_all_territories(
        provider=provider, source=source, year=year, level=level_read
    )

    return territories


def restructure_nested_dict_levels(dict_with_list: dict):

    croisement_decoupage_level_flat = [
        [key, inner_value]
        for key, values in dict_with_list.items()
        for inner_value in values
    ]

    return croisement_decoupage_level_flat


def crossproduct_parameters_production(
    croisement_decoupage_level, list_format, years, crs_list, sources
):

    croisement_decoupage_level_flat = restructure_nested_dict_levels(
        croisement_decoupage_level
    )

    combinations = list(
        itertools.product(
            list_format, croisement_decoupage_level_flat, years, crs_list, sources
        )
    )

    tempdf = pd.DataFrame(
        combinations, columns=["format", "nested", "year", "crs", "source"]
    )
    tempdf["level"] = tempdf["nested"].apply(lambda l: l[0])
    tempdf["decoupage"] = tempdf["nested"].apply(lambda l: l[1])
    tempdf.drop("nested", axis="columns", inplace=True)

    return tempdf


def list_produced_cartiflette(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET
):
    written_levels = fs.glob(f"{bucket}/{path_within_bucket}/**/provider*")
    df = pd.DataFrame(written_levels, columns=['paths'])

    df[
        ['year', 'administrative_level', 'crs', 'decoupage', 'format']
        ] = df['paths'].str.extract(
            r'year=(\d+)/administrative_level=(\w+)/crs=(\d+)/(.*)=.*/vectorfile_format=\'(\w+)\''
            )

    df = df.filter(['year', 'administrative_level', 'crs', 'decoupage', 'format'], axis = 'columns')
    df = df.drop_duplicates()

    with fs.open(f"{bucket}/{path_within_bucket}/available.json", "wb") as f:
        df.to_json(f, orient="records")



def production_cartiflette(
    croisement_decoupage_level, formats, years, crs_list, sources
):

    tempdf = crossproduct_parameters_production(
        croisement_decoupage_level=croisement_decoupage_level,
        list_format=formats,
        years=years,
        crs_list=crs_list,
        sources=sources,
    )

    for index, row in tempdf.iterrows():
        format = row["format"]
        level = row["level"]
        decoupage = row["decoupage"]
        year = row["year"]
        crs = row["crs"]
        source = row["source"]
        print(
            80 * "==" + "\n"
            f"{level=}\n{format=}\n"
            f"{decoupage=}\n{year=}\n"
            f"{crs=}\n"
            f"{source=}"
        )
        
        if level == "COMMUNE_ARRONDISSEMENT":
            production_func = write_vectorfile_s3_custom_arrondissement
        else:
            production_func = write_vectorfile_s3_all
        
        production_func(
            level=level,
            vectorfile_format=format,
            decoupage=decoupage,
            year=year,
            crs=crs,
            provider="IGN",
            source=source,
        )

    print(80 * "-" + "\nProduction finished :)")


def create_nested_topojson(path):

    croisement_decoupage_level = {
        ## structure -> niveau geo: [niveau decoupage macro],
        "REGION": ["FRANCE_ENTIERE"],
        "DEPARTEMENT": ["FRANCE_ENTIERE"],
    }

    croisement_decoupage_level_flat = [
        [key, inner_value]
        for key, values in croisement_decoupage_level.items()
        for inner_value in values
    ]

    list_output = {}
    for couple in croisement_decoupage_level_flat:
        level = couple[0]
        decoupage = couple[1]
        list_output[level] = create_territories(level=level, decoupage=decoupage)

    topo = Topology(
        data=[
            list_output["REGION"]["metropole"],
            list_output["DEPARTEMENT"]["metropole"],
        ],
        object_name=["region", "departement"],
        prequantize=False,
    )

    return topo
    # topo.to_json(path)
