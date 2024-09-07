#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess


def mapshaper_enrich(
    input_geodata_file: str,
    input_metadata_file: str,
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
) -> str:
    """
    Enriches an initial geodata file with additional data using Mapshaper.

    Parameters
    ----------
    input_geodata_file : str
        Path to the input geodata file.
    input_metadata_file : str
        Path to the input metadata file to join to the geodata file.
    output_dir : str
        Directory to store the output file. The default is "temp"
    output_name : str, optional
        The path to write the file to (without extension).
        The default is "concatenated"
    output_format : str, optional
        The format to write the outputfile. The default is "geojson".

    Returns
    -------
    output : str
        Path of the created file

    """

    try:
        os.makedirs(output_dir)
    except FileExistsError:
        pass

    output = f"{output_dir}/{output_name}.{output_format}"

    # Mapshaper command for the enrichment process
    cmd = (
        f"mapshaper {input_geodata_file} "
        "name='' -proj EPSG:4326 "
        f"-join {input_metadata_file} "
        "keys=INSEE_COM,CODGEO "
        "field-types=INSEE_COM:str,CODGEO:str "
        "-filter-fields "
        "INSEE_CAN,INSEE_ARR,SIREN_EPCI,INSEE_DEP,INSEE_REG,NOM_M invert "
        "-rename-fields INSEE_DEP=DEP,INSEE_REG=REG "
        "-each \"PAYS='France'\" "
        f"-o {output} force"
    )

    # Run Mapshaper command
    subprocess.run(
        cmd,
        shell=True,
        check=True,
        text=True,
    )

    return output


# %%
