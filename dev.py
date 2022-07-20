import requests
import yaml
import typing
import tempfile
import zipfile
import py7zr
import shutil

#import os
#os.chdir("cartogether/")
from _download_pb import _download_pb

# Read YAML file
with open("resources/sources.yaml", 'r') as stream:
    dict_open_data = yaml.safe_load(stream)

def safe_download_write(
    url,
    location = None,
    ext = "7z"):

    if location is None:
        tmp = tempfile.NamedTemporaryFile()
        location = tmp.name
        location = location + ext

    _download_pb(url, location)
    #r = requests.get(url)
    #with open(tmp.name, 'w') as f:
    #    f.write(r.content)

    return location


def download_admin_express(
    dict_open_data,
    source: typing.Union[list, str] = ['EXPRESS-COG'],
    year: typing.Optional[str] = None,
    location = None):

    if isinstance(source, list):
        source = source[0]

    dict_source = dict_open_data['IGN']\
        ['ADMINEXPRESS'][source]

    if year is None:
        year = max(
            dict_source.keys()
        )

    url = dict_source[year]["file"]
    
    # download 7z file
    temp_file = tempfile.NamedTemporaryFile()
    temp_file_raw = temp_file.name + ".7z"
    out_name = safe_download_write(url, location = temp_file_raw)

    if location is None:
        tmp = tempfile.TemporaryDirectory()
        location = tmp.name

    # unzip in temp directory
    temp_dir = tempfile.TemporaryDirectory()
    archive = py7zr.SevenZipFile(out_name, mode='r')
    archive.extractall(path=location)
    archive.close()
    
    # copy in location
    shutil.copytree(temp_dir.name, location, dirs_exist_ok=True)

    subdir = url.rsplit("/", maxsplit = 1)[-1]
    subdir = subdir.replace(".7z", "")
    date_livraison = subdir.replace("ADMIN-EXPRESS-COG_3-1__SHP__FRA_L93_", "")

    arbo = f"{location}/{subdir}/ADMIN-EXPRESS-COG/1_DONNEES_LIVRAISON_{date_livraison}"

