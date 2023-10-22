"""Module for communication with Minio S3 Storage
"""

import itertools
import os
import tempfile
import typing
import pandas as pd
import geopandas as gpd
import s3fs
from topojson import Topology

from cartiflette.utils import (
    keep_subset_geopandas,
    dict_corresp_filter_by,
    create_format_standardized,
    create_format_driver,
    create_path_bucket,
    official_epsg_codes,
)

from cartiflette.s3.preprocess import (
    # store_vectorfile_ign,
    get_vectorfile_ign,
    get_vectorfile_communes_arrondissement,
    get_cog_year,
)

from cartiflette import BUCKET, PATH_WITHIN_BUCKET, ENDPOINT_URL, FS


# UTILITIES --------------------------------


def standardize_inputs(vectorfile_format):
    corresp_filter_by_columns = dict_corresp_filter_by()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_write = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_write]

    return corresp_filter_by_columns, format_write, driver


def create_dict_all_territories(
    provider="IGN",
    source="EXPRESS-COG-TERRITOIRE",
    year=2022,
    borders="COMMUNE",
):
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
    simplification: typing.Union[str, int, float] = None
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
        {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "provider": provider,
            "source": source,
            "vectorfile_format": vectorfile_format,
            "borders": borders,
            "filter_by": filter_by,
            "year": year,
            "crs": crs,
            "value": value,
        }
    )

    url = f"{ENDPOINT_URL}/{path_within}"

    print(url)

    return url


# DOWNLOAD FROM S3 --------------------------
# -> moved to /public and refactorized


# UPLOAD S3 -------------------------------


def write_cog_s3(
    year: int = 2022,
    vectorfile_format="json",
    fs: s3fs.S3FileSystem = FS,
):
    list_cog = get_cog_year(year)

    dict_path_data = {
        create_path_bucket(
            {
                "bucket": BUCKET,
                "path_within_bucket": PATH_WITHIN_BUCKET,
                "provider": "INSEE",
                "source": "COG",
                "vectorfile_format": vectorfile_format,
                "borders": level,
                "filter_by": "france_entiere",
                "year": year,
                "value": "raw",
                "crs": None,
                "simplification": None
            }
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
                print("Unsupported format")


def write_vectorfile_subset(
    object: gpd.GeoDataFrame,
    value: str = "28",
    vectorfile_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    year: int = 2022,
    crs: typing.Union[str, int, float] = 2154,
    force_crs: bool = False,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = FS,
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

    Returns
    -------
    None
    """

    corresp_filter_by_columns, format_write, driver = standardize_inputs(
        vectorfile_format
    )

    write_path = create_path_bucket(
        {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "provider": provider,
            "source": source,
            "vectorfile_format": format_write,
            "borders": borders,
            "filter_by": filter_by,
            "year": year,
            "value": value,
            "crs": crs,
        }
    )

    print(f"Writing file at {write_path} location")

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
            object=object_subset, fs=FS, write_path=write_path, driver=driver
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


def write_vectorfile_all_borders(
    object: gpd.GeoDataFrame,
    borders_var: str,
    borders: str = "COMMUNE",
    vectorfile_format: str = "geojson",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = 2022,
    crs: typing.Union[str, int, float] = 2154,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = FS,
):
    """Write all borders of a GeoDataFrame to a specified format on S3.

    This function takes a GeoDataFrame object, the variable name on which to create
    the borders and other parameters like format, filter_by, year, bucket and path
    within the bucket, crs and borders for the vector file to be written on S3.

    Args:
        object (gpd.GeoDataFrame): The GeoDataFrame object to write.
        borders_var (str): The variable name on which to create the borders.
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
    filter_by="region",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    crs=2154,
    borders=None,  # used to ensure we produce for "metropole" only
    fs: s3fs.S3FileSystem = FS,
):
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
        fs=fs,
    )


# main function


def write_vectorfile_s3_all(
    borders="COMMUNE",
    vectorfile_format="geojson",
    filter_by="region",
    year=2022,
    crs: int = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider="IGN",
    source="EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = FS,
):
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
        print(f"Writing {territory}")

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
            fs=fs,
        )


def open_vectorfile_from_s3(
    vectorfile_format,
    filter_by,
    year,
    value,
    crs,
    fs: s3fs.S3FileSystem = FS,
):
    read_path = create_path_bucket(
        {
            "vectorfile_format": vectorfile_format,
            "filter_by": filter_by,
            "year": year,
            "value": value,
            "crs": crs,
        }
    )
    return fs.open(read_path, mode="r")


def write_vectorfile_from_s3(
    filename: str,
    filter_by: str,
    year: int,
    value: str,
    vectorfile_format: str = "geojson",
    crs: int = 2154,
    provider="IGN",
    source="EXPRESS-COG-TERRITOIRE",
    fs: s3fs.S3FileSystem = FS,
):
    """Retrieve shapefiles stored in S3

    Args:
        filename (str): Filename
        filter_by (str): _description_
        year (int): Year that should be used
        value (str): Which value should be retrieved
        vectorfile_format (str, optional): vectorfile format needed. Defaults to "geojson".
    """

    read_path = create_path_bucket(
        {
            "vectorfile_format": vectorfile_format,
            "filter_by": filter_by,
            "year": year,
            "value": value,
            "crs": crs,
            "provider": provider,
            "source": source,
        }
    )

    fs.download(read_path, filename)

    print(f"Requested file has been saved at location {filename}")


def create_territories(
    borders: str = "COMMUNE",
    filter_by: str = "region",
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

    corresp_filter_by_columns = dict_corresp_filter_by()

    var_filter_by_s3 = corresp_filter_by_columns[filter_by.lower()]
    borders_read = borders.upper()

    # IMPORT SHAPEFILES ------------------

    territories = create_dict_all_territories(
        provider=provider, source=source, year=year, borders=borders_read
    )

    return territories


def restructure_nested_dict_borders(dict_with_list: dict):
    croisement_filter_by_borders_flat = [
        [key, inner_value]
        for key, values in dict_with_list.items()
        for inner_value in values
    ]

    return croisement_filter_by_borders_flat


def crossproduct_parameters_production(
    croisement_filter_by_borders, list_format, years, crs_list, sources
):
    croisement_filter_by_borders_flat = restructure_nested_dict_borders(
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
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
):
    written_borders = fs.glob(f"{bucket}/{path_within_bucket}/**/provider*")
    df = pd.DataFrame(written_borders, columns=["paths"])

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
    fs: s3fs.S3FileSystem = FS,
):
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
        print(
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
            fs=fs,
        )

    print(80 * "-" + "\nProduction finished :)")


def create_nested_topojson(path):
    croisement_filter_by_borders = {
        # structure -> niveau geo: [niveau filter_by macro],
        "REGION": ["FRANCE_ENTIERE"],
        "DEPARTEMENT": ["FRANCE_ENTIERE"],
    }

    croisement_filter_by_borders_flat = [
        [key, inner_value]
        for key, values in croisement_filter_by_borders.items()
        for inner_value in values
    ]

    list_output = {}
    for couple in croisement_filter_by_borders_flat:
        borders = couple[0]
        filter_by = couple[1]
        list_output[borders] = create_territories(
            borders=borders, filter_by=filter_by
        )

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
