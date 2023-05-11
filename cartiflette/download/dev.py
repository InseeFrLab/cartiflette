"""
Request data from IGN and other tile providers
"""
import os
import ftplib
import glob
import typing
import tempfile
import zipfile
import py7zr
import numpy as np
import pandas as pd
import geopandas as gpd


from cartiflette.utils import (
    download_pb,
    download_pb_ftp,
    import_yaml_config,
    url_express_COG_territoire,
)


def safe_download_write(
    url: str,
    location: str = None,
    param_ftp: dict = None,
    ext: str = "7z",
    verify: bool = True,
    force=True,
) -> str:
    """
    Download data given URL and additional parameters.

    File is downloaded either using requests or ftplib

    Args:
        url (str): URL from which data should be fetched. Depending
          on the type of URL (http/https protocole or FTP),
          either request or ftplib will be used to download
          dataset.
        location (str, optional): Location where the file should be written.
          Defaults to None means a temporary file is used.
        param_ftp (dict, optional): Dictionary with parameters useful
          for FTP download. Ignored if the file is not located on a
          FTP server. Defaults to None.
        ext (str, optional): File extension. Defaults to "7z".

    Returns:
        str: File location
    """

    if location is None:
        tmp = tempfile.NamedTemporaryFile()
        location = tmp.name
        location = location + ext

    if param_ftp is not None:
        ftp = ftplib.FTP(param_ftp["hostname"], param_ftp["username"], param_ftp["pwd"])
        download_pb_ftp(ftp, url, fname=location)
    else:
        download_pb(url, location, verify=verify, force=force)

    return location


def create_url_adminexpress(
    provider: typing.Union[list, str] = ["IGN", "opendatarchives"],
    source: typing.Union[list, str] = ["EXPRESS-COG", "EXPRESS-COG-TERRITOIRE"],
    year: typing.Optional[str] = None,
    field: str = "metropole",
):
    """Create a standardized url to find the relevent IGN
      source

    Args:
        provider (typing.Union[list, str], optional): IGN data provider.
            Defaults to 'IGN' but can be 'opendatarchives'
            (contributive back-up).
        source (typing.Union[list, str], optional): Sources used.
         Can either be a string or a list. Defaults to ['EXPRESS-COG'].
        year (typing.Optional[str], optional): Year to use. Defaults to None.

    Returns:
        _type_: _description_
    """

    if isinstance(provider, list):
        provider = provider[0]
    if isinstance(source, list):
        source = source[0]
    if year is None:
        year = 2022

    dict_open_data = import_yaml_config()
    dict_source = dict_open_data[provider]["ADMINEXPRESS"][source]

    if source.endswith("-TERRITOIRE"):
        url = url_express_COG_territoire(year=year, provider=provider, territoire=field)
    else:
        url = dict_source[year]["file"]

    return url


def download_admin_express(
    provider: typing.Union[list, str] = ["IGN", "opendatarchives"],
    source: typing.Union[list, str] = ["EXPRESS-COG", "EXPRESS-COG-TERRITOIRE"],
    year: typing.Optional[str] = None,
    location: str = None,
    field: str = "metropole",
) -> str:
    """
    Download AdminExpress data for a given type of source and year

    Args:
        source (typing.Union[list, str], optional): Sources used.
         Can either be a string or a list. Defaults to ['EXPRESS-COG'].
         year (typing.Optional[str], optional): Year to use. Defaults to None.
        location (str, optional): Location where file should be written.
          Defaults to None.
        provider (typing.Union[list, str], optional): IGN data provider.
            Defaults to 'IGN' but can be 'opendatarchives'
            (contributive back-up).

    Returns:
        str: Complete path where the IGN source has been unzipped.
    """

    if isinstance(provider, list):
        provider: str = provider[0]
    if isinstance(source, list):
        source: str = source[0]

    dict_open_data = import_yaml_config()
    dict_source = dict_open_data[provider]["ADMINEXPRESS"][source]

    url = create_url_adminexpress(
        provider=provider, year=year, source=source, field=field
    )
    print(url)

    if url.startswith(("http", "https")):
        param_ftp = None
    else:
        param_ftp = dict_source["FTP"]

    if location is not None and os.path.isdir(location):
        print(
            f"Data have been previously downloaded and are still available in {location}"
        )
    else:
        # download 7z file
        temp_file = tempfile.NamedTemporaryFile()
        temp_file_raw = temp_file.name + ".7z"
        out_name = safe_download_write(url, location=temp_file_raw, param_ftp=param_ftp)
        if location is None:
            tmp = tempfile.TemporaryDirectory()
            location = tmp.name
        # unzip in location directory
        archive = py7zr.SevenZipFile(out_name, mode="r")
        archive.extractall(path=location)
        archive.close()

    arbo = glob.glob(f"{location}/**/1_DONNEES_LIVRAISON_*", recursive=True)

    return arbo


def download_store_admin_express(
    source: typing.Union[list, str] = ["EXPRESS-COG", "COG", "EXPRESS-COG-TERRITOIRE"],
    year: typing.Optional[str] = None,
    location: str = None,
    provider: typing.Union[list, str] = ["IGN", "opendatarchives"],
    field: str = "metropole",
) -> str:
    """
    Download, unzip and store AdminExpress data

    Args:
        source (typing.Union[list, str], optional): IGN data product. Defaults to ['EXPRESS-COG'].
        year (typing.Optional[str], optional): Year used. Defaults to None.
        location (str, optional): File location. Defaults to None.
        provider (typing.Union[list, str], optional): IGN data provider. Defaults to 'IGN' but can be 'opendatarchives'
            (contributive back-up).

    Returns:
        str: _description_
    """

    if isinstance(source, list):
        source: str = source[0]
    if isinstance(provider, list):
        provider: str = provider[0]

    dict_open_data = import_yaml_config()

    # print(provider)
    # print(source)

    dict_source = dict_open_data[provider]["ADMINEXPRESS"][source]

    if year is None:
        year = max([i for i in dict_source.keys() if i not in ("field", "FTP")])

    if location is None:
        location = tempfile.gettempdir()
        location = f"{location}/{source}-{year}"
        if year > 2020:
            if source.endswith("-TERRITOIRE"):
                location = f"{location}/{field}"

    path_cache_ign = download_admin_express(
        source=source, year=year, location=location, provider=provider, field=field
    )

    # For some years, md5 also validate pattern DONNEES_LIVRAISON
    path_cache_ign = [s for s in path_cache_ign if not s.endswith(".md5")]
    path_cache_ign = path_cache_ign[0]

    if year <= 2020 and source.endswith("-TERRITOIRE"):
        field_code = dict_source["field"][field].split("_")[0]
        path_cache_ign = glob.glob(f"{path_cache_ign}/*{field_code}_*")
        # For some years, md5 also validate pattern DONNEES_LIVRAISON
        path_cache_ign = [s for s in path_cache_ign if not s.endswith(".md5")]
        path_cache_ign = path_cache_ign[0]

    return path_cache_ign


def store_vectorfile_ign(
    source: typing.Union[list, str] = ["EXPRESS-COG", "COG", "EXPRESS-COG-TERRITOIRE"],
    year: typing.Optional[str] = None,
    field: str = "metropole",
    provider: typing.Union[list, str] = ["IGN", "opendatarchives"],
) -> str:
    """
    Function to download raw IGN shapefiles and
    store them unzipped in filesystem

    Args:
        source (typing.Union[list, str], optional): IGN data product.
            Defaults to ['EXPRESS-COG'].
        year (typing.Optional[str], optional): Year used. Defaults to None.
        field (str, optional): Geographic level to use. Defaults to "metropole".
        provider (typing.Union[list, str], optional): IGN data provider.
            Defaults to 'IGN' but can be 'opendatarchives'
            (contributive back-up).

    Returns:
        str: Returns where file is stored on filesystem.
    """

    path_cache_ign = download_store_admin_express(
        source=source, year=year, provider=provider, field=field
    )
    # returns path where datasets are stored

    full_path_shp = glob.glob(f"{path_cache_ign}/**/*.shp", recursive=True)
    shp_location = os.path.dirname(full_path_shp[0])

    return shp_location


def get_administrative_level_available_ign(
    source: typing.Union[list, str] = ["EXPRESS-COG", "EXPRESS-COG-TERRITOIRE"],
    year: typing.Optional[str] = None,
    field: typing.Union[list, str] = [
        "metropole",
        "guadeloupe",
        "martinique",
        "reunion",
        "guyane",
        "mayotte",
    ],
    verbose: bool = True,
) -> list:
    """
    User-level function to get administrative data that are available
     in IGN raw sources for a given year

    Args:
        source (typing.Union[list, str], optional): IGN data product. Defaults to ['EXPRESS-COG'].
        year (typing.Optional[str], optional): Year used. Defaults to None.
        field (typing.Union[list, str], optional): _description_.
           Defaults to "metropole". Acceptable values are "metropole",
           "guadeloupe", "martinique", "reunion", "guyane", "mayotte"].
        verbose (bool, optional): Should we print values or just return
           them as list ? Defaults to True.

    Returns:
        list: List of administrative levels available
    """

    dict_open_data = import_yaml_config()

    if isinstance(source, list):
        source = source[0]

    dict_source = dict_open_data["IGN"]["ADMINEXPRESS"][source]

    if year is None:
        year = max([i for i in dict_source.keys() if i not in ("field", "FTP")])

    if isinstance(field, list):
        field = field[0]

    shp_location = store_vectorfile_ign(source=source, year=year, field=field)

    list_levels = [
        os.path.basename(i).replace(".shp", "")
        for i in glob.glob(shp_location + "/*.shp")
    ]
    if verbose:
        print("\n  - ".join(["Available administrative levels are :"] + list_levels))
    return list_levels


def get_vectorfile_ign(
    source: typing.Union[list, str] = ["EXPRESS-COG"],
    year: typing.Optional[str] = None,
    field: typing.Union[list, str] = [
        "metropole",
        "guadeloupe",
        "martinique",
        "reunion",
        "guyane",
        "mayotte",
    ],
    borders: typing.Union[list, str] = ["COMMUNE"],
    provider: typing.Union[list, str] = ["IGN", "opendatarchives"],
) -> gpd.GeoDataFrame:
    """
    User-level function to get shapefiles from IGN

    Args:
        source (typing.Union[list, str], optional): IGN data product. Defaults to ['EXPRESS-COG'].
        year (typing.Optional[str], optional): Year used. Defaults to None.
        field (typing.Union[list, str], optional): _description_.
           Defaults to "metropole". Acceptable values are "metropole",
           "guadeloupe", "martinique", "reunion", "guyane", "mayotte"].
        level (typing.Union[list, str], optional): Administrative level. Defaults to ['COMMUNE'].

    Returns:
        gpd.GeoDataFrame : _description_
    """

    if isinstance(source, list):
        source: str = source[0]

    if isinstance(borders, list):
        level: str = borders[0]

    if isinstance(provider, list):
        provider: str = provider[0]

    if isinstance(field, list):
        field = field[0]

    if year == 2019:
        field = "metropole"

    shp_location = store_vectorfile_ign(
        source=source, year=year, field=field, provider=provider
    )

    data_ign = gpd.read_file(f"{shp_location}/{borders}.shp")

    if borders == "ARRONDISSEMENT_MUNICIPAL":
        data_ign["INSEE_DEP"] = data_ign["INSEE_COM"].str[:2]

    data_ign["source"] = f"{provider}:{source}"

    return data_ign


def get_vectorfile_communes_arrondissement(
    year=2022, provider="IGN", source="EXPRESS-COG-TERRITOIRE"
):
    arrondissement = get_vectorfile_ign(
        borders="ARRONDISSEMENT_MUNICIPAL",
        year=year,
        field="metropole",
        provider=provider,
        source=source,
    )
    communes = get_vectorfile_ign(
        borders="COMMUNE",
        year=year,
        field="metropole",
        provider=provider,
        source=source,
    )
    communes_sans_grandes_villes = communes.loc[
        ~communes["NOM"].isin(["Marseille", "Lyon", "Paris"])
    ]
    communes_grandes_villes = communes.loc[
        communes["NOM"].isin(["Marseille", "Lyon", "Paris"])
    ]

    arrondissement_extra_info = arrondissement.merge(
        communes_grandes_villes, on="INSEE_DEP", suffixes=("", "_y")
    )
    arrondissement_extra_info = arrondissement_extra_info.loc[
        :, ~arrondissement_extra_info.columns.str.endswith("_y")
    ]

    df_enrichi = pd.concat(
        [
            communes_sans_grandes_villes,
            arrondissement_extra_info
        ]
        )

    df_enrichi["INSEE_COG"] = np.where(
        df_enrichi["INSEE_ARM"].isnull(),
        df_enrichi["INSEE_COM"],
        df_enrichi["INSEE_ARM"],
    )

    df_enrichi = df_enrichi.drop("INSEE_ARM", axis="columns")

    return df_enrichi


def get_cog_year(year: int = 2022):
    config = import_yaml_config()

    config_cog_year = config["Insee"]["COG"][year]
    config_root = config["Insee"]["COG"]["root"]
    url_root = f"{config_root}/{config_cog_year['id']}"

    urls = {
        cog["alias"]: f"{url_root}/{cog['filename']}"
        for key, cog in config_cog_year["content"].items()
    }

    dict_cog = {
        level: pd.read_csv(url)\
            for level, url in urls.items()
    }

    return dict_cog


def get_BV(year: int = 2022):
    """
    Import and Unzip Bassins de vie (Insee, format 2012)

    Args:
        year

    Returns:
        A DataFrame
    """

    dict_open_data = import_yaml_config()

    url = dict_open_data["Insee"]["BV2012"][year]["file"]

    # from dev import safe_download_write
    out_name = safe_download_write(
        url, location=None, param_ftp=None, ext=".zip", verify=False, force=True
    )

    tmp = tempfile.TemporaryDirectory()
    location = tmp.name
    # unzip in location directory

    archive = zipfile.ZipFile(out_name, "r")
    archive.extractall(path=location)
    archive.close()

    df = pd.read_excel(
        location + "/" + dict_open_data["Insee"]["BV2012"][year]["excel_name"],
        sheet_name="Composition_communale",
        skiprows=5,
    )
    df = df.loc[df["BV2012"] != "ZZZZZ"][["CODGEO", "BV2012"]]
    # ZZZZZ à Mayotte

    return df


# def get_bv(location):
#    dict_open_data = import_yaml_config()
#    url = dict_open_data['Insee']\
#        ['BV']['2022']["file"]
#
#    if location is not None and os.path.isdir(location):
#        print(f"Data have been previously downloaded and are still available in {location}")
#    else:
#
#    safe_download_write(
#    url: str,
#    location: str = None,
#    param_ftp: dict = None,
#    ext: str = "7z")
