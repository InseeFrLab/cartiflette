"""
Request data from IGN and other tile providers
"""
import os
import ftplib
import glob
import re
import typing
import tempfile
import py7zr
import geopandas as gpd


from cartiflette.utils import download_pb, download_pb_ftp, import_yaml_config


def safe_download_write(
    url: str, location: str = None, param_ftp: dict = None, ext: str = "7z"
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
        ftp = ftplib.FTP(
            param_ftp["hostname"],
            param_ftp["username"],
            param_ftp["pwd"]
            )
        download_pb_ftp(ftp, url, fname=location)
    else:
        download_pb(url, location)

    return location

def create_url_adminexpress(
    provider: typing.Union[list, str] = ['IGN','opendatarchives'],
    source: typing.Union[list, str] = ["EXPRESS-COG"],
    year: typing.Optional[str] = None
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
    dict_open_data = import_yaml_config()
    dict_source = dict_open_data[provider]["ADMINEXPRESS"][source]
    url = dict_source[year]["file"]
    return url


def download_admin_express(
    provider: typing.Union[list, str] = ['IGN', 'opendatarchives'],
    source: typing.Union[list, str] = ["EXPRESS-COG"],
    year: typing.Optional[str] = None,
    location: str = None,
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
        provider=provider,
        year=year,
        source=source
    )

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
        out_name = safe_download_write(
            url,
            location=temp_file_raw,
            param_ftp=param_ftp)
        if location is None:
            tmp = tempfile.TemporaryDirectory()
            location = tmp.name
        # unzip in location directory
        archive = py7zr.SevenZipFile(out_name, mode="r")
        archive.extractall(path=location)
        archive.close()

    subdir = url.rsplit("/", maxsplit=1)[-1]
    subdir = subdir.replace(".7z", "")
    if url.startswith(("http", "https")) and provider == "IGN" is False :
        subdir = subdir.replace("_L93", "")  # 2021: L93 en trop
        subdir = subdir.replace("_WGS84G", "")  # 2019: WGS84 en trop
        subdir = subdir.replace(".001", "")

    date_livraison = subdir.rsplit("_", maxsplit=1)[-1]
    arbo = f"{location}/{subdir}/ADMIN-EXPRESS-COG"

    if os.path.exists(arbo) is False:
        path_to_check = glob.glob(f"{location}/**/ADMIN-EXPRESS-COG*")
        if not path_to_check:
            path_to_check = glob.glob(f"{location}/**/ADMIN-EXPRESS*")
        # if we don't find arbo, we use the bulldozer
        subdirs = set(
            [
                x.replace(".md5", "")
                for x in path_to_check
            ]
        )
        arbo = list(subdirs)[0]

    arbo_complete = f"{arbo}/1_DONNEES_LIVRAISON_{date_livraison}"

    if os.path.exists(arbo_complete) is False:
        # sometimes we have a different livraison date
        subdirs = [
            os.path.basename(x).replace(".md5", "")
            for x in glob.glob(f"{arbo}/1_DONNEES_LIVRAISON_*")
        ]
        date_livraison_subdir = [i for i in set(subdirs)][0]
        date_livraison_subdir = date_livraison_subdir.rsplit("_", maxsplit=1)[-1]
        arbo_complete = f"{arbo}/1_DONNEES_LIVRAISON_{date_livraison_subdir}"

    return arbo_complete


def download_store_admin_express(
    source: typing.Union[list, str] = ["EXPRESS-COG", "COG"],
    year: typing.Optional[str] = None,
    location: str = None,
    provider: typing.Union[list, str] = ['IGN', 'opendatarchives']
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

    print(provider)
    print(source)

    dict_source = dict_open_data[provider]["ADMINEXPRESS"][source]

    if year is None:
        year = max(dict_source.keys())

    if location is None:
        location = tempfile.gettempdir()
        location = f"{location}/{source}-{year}"

    path_cache_ign = download_admin_express(
        source=source,
        year=year,
        location=location,
        provider=provider
        )

    return path_cache_ign


def import_ign_vectorfile(
    source: typing.Union[list, str] = ["EXPRESS-COG","COG"],
    year: typing.Optional[str] = None,
    field: str = "metropole",
    provider: typing.Union[list, str] = ['IGN', 'opendatarchives']
) -> str:
    """
    Function to download raw IGN shapefiles and store them unzipped in filesystem

    Args:
        source (typing.Union[list, str], optional): IGN data product. Defaults to ['EXPRESS-COG'].
        year (typing.Optional[str], optional): Year used. Defaults to None.
        field (str, optional): Geographic level to use. Defaults to "metropole".
        provider (typing.Union[list, str], optional): IGN data provider. Defaults to 'IGN' but can be 'opendatarchives'
            (contributive back-up).

    Returns:
        str: Returns where file is stored on filesystem.
    """

    dict_open_data = import_yaml_config()
    path_cache_ign = download_store_admin_express(
        source=source,
        year=year,
        provider=provider)

    ign_code_level = dict_open_data[provider]["ADMINEXPRESS"][source]["field"]

    matching_pattern_group = re.search(
        "/ADMIN-EXPRESS-COG_(.*)__SHP", path_cache_ign)
    if matching_pattern_group is None:
        matching_pattern_group = re.search(
            "/ADMIN-EXPRESS_(.*)__SHP", path_cache_ign)

    ign_version = matching_pattern_group.group(1)

    if year < 2022:
        ign_code_level["prefix"] = ign_code_level["prefix"].replace(
            "3-1_", f"{ign_version}_"
        )

    if year == 2019:
        ign_code_level[field] = ign_code_level[field].replace("LAMB93", "WGS84")

    shp_location = f"{path_cache_ign}/{ign_code_level['prefix']}"
    shp_location = f"{shp_location}{ign_code_level[field]}"

    if os.path.isdir(shp_location) is False:
        # sometimes, ADECOG is spelled ADE-COG
        shp_location = shp_location.replace("ADECOG", "ADE-COG")

    if not os.path.exists(shp_location):
        # sometimes it is not even ADECOG
        subdirs = os.listdir(path_cache_ign)
        subdirs = [s for s in subdirs if not s.endswith("md5")][0]
        shp_location = f"{path_cache_ign}/{subdirs}"

    if os.path.isdir(shp_location) is False:
        # for some years, geographic codes were not the same
        dep_code = ign_code_level[field].rsplit("_", maxsplit=1)[-1]
        filename = glob.glob(f"{os.path.dirname(shp_location)}/*_{dep_code}")
        shp_location = filename[0]

    return shp_location


def get_administrative_level_available_ign(
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

    shp_location = import_ign_vectorfile(source=source, year=year, field=field)

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
    level: typing.Union[list, str] = ["COMMUNE"],
    provider: typing.Union[list, str] = ['IGN', 'opendatarchives']
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
    dict_open_data = import_yaml_config()

    if isinstance(source, list):
        source: str = source[0]

    if isinstance(level, list):
        level: str = level[0]

    if isinstance(provider, list):
        provider: str = provider[0]

    dict_source = dict_open_data[provider]["ADMINEXPRESS"][source]

    if year is None:
        year = max([i for i in dict_source.keys() if i not in ("field", "FTP")])

    if isinstance(field, list):
        field = field[0]

    if year == 2019:
        field = "metropole"

    shp_location = import_ign_vectorfile(source=source, year=year, field=field, provider=provider)

    data_ign = gpd.read_file(f"{shp_location}/{level}.shp")

    if level == "ARRONDISSEMENT_MUNICIPAL":
        data_ign["INSEE_DEP"] = data_ign['INSEE_COM'].str[:2]

    return data_ign


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
