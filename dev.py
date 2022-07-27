import requests
import yaml
import typing
import tempfile
import zipfile
import py7zr
import shutil
import glob
import os
import re
import ftplib
import geopandas as gpd


#os.chdir("cartogether/")
from _download_pb import _download_pb, _download_pb_ftp

# Read YAML file
def import_yaml_config(location = "resources/sources.yaml"):
    with open(location, 'r') as stream:
        dict_open_data = yaml.safe_load(stream)
    return dict_open_data

def safe_download_write(
    url,
    location = None,
    param_ftp = None,
    ext = "7z"):

    if location is None:
        tmp = tempfile.NamedTemporaryFile()
        location = tmp.name
        location = location + ext

    if param_ftp is not None:
        ftp = ftplib.FTP(param_ftp['hostname'], param_ftp['username'], param_ftp['pwd'])
        _download_pb_ftp(ftp, url, fname = location)
    else:
        _download_pb(url, location)
    #r = requests.get(url)
    #with open(tmp.name, 'w') as f:
    #    f.write(r.content)

    return location





def download_admin_express(
    source: typing.Union[list, str] = ['EXPRESS-COG'],
    year: typing.Optional[str] = None,
    location = None):

    dict_open_data = import_yaml_config()

    dict_source = dict_open_data['IGN']\
        ['ADMINEXPRESS'][source]

    url = dict_source[year]["file"]

    if url.startswith('http'):
        param_ftp = None
    else:
        param_ftp = dict_source["FTP"]


    if location is not None and os.path.isdir(location):
       print(f"Data have been previously downloaded and are still available in {location}")
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
        temp_dir = tempfile.TemporaryDirectory()
        archive = py7zr.SevenZipFile(out_name, mode='r')
        archive.extractall(path=location)
        archive.close()
    
    subdir = url.rsplit("/", maxsplit=1)[-1]
    subdir = subdir.replace(".7z", "")
    if url.startswith('http') is False:
        subdir = subdir.replace("_L93", "") #2021: L93 en trop

    date_livraison = subdir.rsplit("_", maxsplit=1)[-1]
    arbo = f"{location}/{subdir}/ADMIN-EXPRESS-COG/1_DONNEES_LIVRAISON_{date_livraison}"

    return arbo


def download_store_admin_express(
    source: typing.Union[list, str] = ['EXPRESS-COG'],
    year: typing.Optional[str] = None,
    location = None):

    if isinstance(source, list):
        source = source[0]

    if year is None:
        year = max(
            dict_source.keys()
        )

    if location is None:
        location = tempfile.gettempdir()
        location = f"{location}/{source}-{year}"

    path_cache_ign = download_admin_express(
        source = source,
        year = year,
        location = location
    )

    return path_cache_ign



def import_ign_shapefile(
    source: typing.Union[list, str] = ['EXPRESS-COG'],
    year: typing.Optional[str] = None,
    field: str = "metropole"
    ):

    dict_open_data = import_yaml_config()
    path_cache_ign = download_store_admin_express(source, year)

    ign_code_level = dict_open_data['IGN']\
        ['ADMINEXPRESS'][source]['field']

    ign_version = re.search('/ADMIN-EXPRESS-COG_(.*)__SHP', path_cache_ign).group(1)
    
    if year < 2022:
        ign_code_level['prefix'] = ign_code_level['prefix'].replace("3-1_", f"{ign_version}_")

    shp_location = f"{path_cache_ign}/{ign_code_level['prefix']}{ign_code_level[field]}"

    if os.path.isdir(shp_location) is False:
        # sometimes, ADECOG is spelled ADE-COG
        shp_location = shp_location.replace("ADECOG", "ADE-COG")
    
    return shp_location


def get_administrative_level_available_ign(
    source: typing.Union[list, str] = ['EXPRESS-COG'],
    year: typing.Optional[str] = None,
    field: typing.Union[list, str] = ["metropole", "guadeloupe", "martinique", "reunion", "guyane", "mayotte"],
    verbose: bool = True):

    dict_open_data = import_yaml_config()

    if isinstance(source, list):
        source = source[0]

    dict_source = dict_open_data['IGN']\
        ['ADMINEXPRESS'][source]


    if year is None:
        year = max(
            [i for i in dict_source.keys() if i not in ("field", "FTP")]
        )

    if isinstance(field, list):
        field = field[0]

    shp_location = import_ign_shapefile(
        source = source,
        year = year,
        field = field
    )

    list_levels = [os.path.basename(i).replace(".shp", "") for i in glob.glob(shp_location + "/*.shp")]
    if verbose:
        print(
            "\n  - ".join(["Available administrative levels are :"] + list_levels)
        )
    return list_levels

def get_shapefile_ign(
    source: typing.Union[list, str] = ['EXPRESS-COG'],
    year: typing.Optional[str] = None,
    field: typing.Union[list, str] = ["metropole", "guadeloupe", "martinique", "reunion", "guyane", "mayotte"],
    level: typing.Union[list, str] = ['COMMUNE']
):
    dict_open_data = import_yaml_config()

    if isinstance(source, list):
        source = source[0]

    if isinstance(level, list):
        level = level[0]

    dict_source = dict_open_data['IGN']\
        ['ADMINEXPRESS'][source]


    if year is None:
        year = max(
            [i for i in dict_source.keys() if i not in ("field", "FTP")]
        )

    if isinstance(field, list):
        field = field[0]

    shp_location = import_ign_shapefile(
            source = source,
            year = year,
            field = field
        )

    df = gpd.read_file(f'{shp_location}/{level}.shp')

    return df







    


