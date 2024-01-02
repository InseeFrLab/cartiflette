import os

from cartiflette.config import FS
from cartiflette.s3 import list_raw_files_level, download_files_from_list


def prepare_local_directory_mapshaper(
    path_bucket,
    borders="COMMUNE",
    territory="metropole",
    niveau_agreg="DEPARTEMENT",
    format_output="topojson",
    simplification=0,
    local_dir="temp",
    fs=FS,
):
    """
    Prepares the local directory for processing with Mapshaper.

    This function creates a local directory structure and downloads
      raw shapefiles from the specified path in the file system.

    Parameters
    ----------
    path_bucket : str
        The path to the bucket in the file system.
    borders : str, optional
        The type of borders, by default "COMMUNE".
    niveau_agreg : str, optional
        The level of aggregation, by default "DEPARTEMENT".
    format_output : str, optional
        The output format, by default "topojson".
    simplification : int, optional
        The degree of simplification, by default 0.
    local_dir : str, optional
        The local directory for file storage, by default "temp".
    fs : FileSystem, optional
        The file system object, by default fs.

    Returns
    -------
    dict
        A dictionary containing paths for the original and destination directories.

    """
    local_dir = f"{local_dir}/{territory}"
    os.makedirs(local_dir, exist_ok=True)
    # Get all raw shapefiles from Minio
    list_raw_files = list_raw_files_level(fs, path_bucket, borders=borders)
    download_files_from_list(fs, list_raw_files, local_dir=local_dir)
    local_path_destination = (
        f"{local_dir}/{niveau_agreg}/{format_output}/{simplification=}"
    )
    os.makedirs(local_path_destination, exist_ok=True)
    paths = {"path_origin": local_dir, "path_destination": local_path_destination}
    return paths
