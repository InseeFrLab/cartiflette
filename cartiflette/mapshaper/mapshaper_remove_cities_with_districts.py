#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 17:15:30 2024

@author: thomasgrandjean
"""

import os
import subprocess


def mapshaper_remove_cities_with_districts(
    input_city_file: str,
    dir_output: str = "temp",
    name_output: str = "output",
    format_output: str = "geojson",
):
    """
    Remove cities with communal districts (Paris, Lyon, Marseille) from the
    base cities geodataset.

    Parameters
    ----------
    input_city_file : str
        Path to the input file.
    name_output : str, optional
        Name of the written file, without extension. The default is "output".
    format_output : str, optional
        Format for output file. The default is "geojson".

    Returns
    -------
    None.

    """
    try:
        os.makedirs(dir_output)
    except FileExistsError:
        pass

    cmd = (
        # TODO : not working on windows ?!
        f"mapshaper {input_city_file} name='COMMUNE' -proj EPSG:4326 "
        "-filter '\"69123,13055,75056\".indexOf(INSEE_COM) > -1' invert "
        '-each "INSEE_COG=INSEE_COM" '
        "-o force "
        f"{dir_output}/{name_output}.{format_output} "
        f'format={format_output} extension=".{format_output}" singles'
    )
    subprocess.run(cmd, shell=True, check=True, text=True)
