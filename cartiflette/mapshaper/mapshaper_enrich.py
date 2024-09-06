"""
Data wrangling (geo)operations wrappers from mapshaper.
"""

import os
import subprocess

from cartiflette.utils import DICT_CORRESP_ADMINEXPRESS


def mapshaper_enrich(
    local_dir: str = "temp",
    filename_initial: str = "COMMUNE.shp",
    output_path: str = None,
    metadata_file: str = "temp/tagc.csv",
    dict_corresp: dict = DICT_CORRESP_ADMINEXPRESS,
) -> None:
    """
    Enriches an initial shapefile with additional data using Mapshaper and a specified
    correspondence dictionary.

    Parameters:
    - local_dir (str): The local directory where the initial shapefile is stored and
      Mapshaper will be executed (default is "temp").
    - filename_initial (str): The name of the initial shapefile without extension
      (default is "COMMUNE").
    - output_path (str): The path for the output file after enrichment. If
      None, the file will be overwritten (default is None).
    - metadata_file (str): The local path to a metadata
      datafile to join to the initial geodata file.
      metadatafile or a DataFrame object of the metadatafile to enrich
    - dict_corresp (dict): A dictionary containing correspondences for field renaming
      and value assignment (default is DICT_CORRESP_ADMINEXPRESS).

    Returns:
    - None: The function runs Mapshaper with the specified commands and enriches
      the initial shapefile.
    """

    force = False
    if not output_path:
        force = True
        output_path = f"{local_dir}/{filename_initial}"

    # Mapshaper command for the enrichment process
    cmd = (
        f"mapshaper {local_dir}/{filename_initial} "
        f"name='' -proj EPSG:4326 "
        f"-join {metadata_file} "
        f"keys=INSEE_COM,CODGEO field-types=INSEE_COM:str,CODGEO:str "
        f"-filter-fields INSEE_CAN,INSEE_ARR,SIREN_EPCI,INSEE_DEP,INSEE_REG,NOM_M invert "
        f"-rename-fields INSEE_DEP=DEP,INSEE_REG=REG "
        f"-each \"{dict_corresp['FRANCE_ENTIERE']}='France'\" "
        f"-o {output_path}"
    )
    if force:
        cmd += " force"

    # Run Mapshaper command
    subprocess.run(
        cmd,
        shell=True,
        check=True,
        text=True,
    )


# %%
