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
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
) -> str:
    """
    Remove cities with communal districts (Paris, Lyon, Marseille) from the
    base cities geodataset.

    Parameters
    ----------
    input_city_file : str
        Path to the input file.
    output_dir : str
        Directory to store the output file. The default is "temp".
    output_name : str, optional
        Name of the written file, without extension. The default is "output".
    output_format : str, optional
        Format for output file. The default is "geojson".

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

    cmd = (
        f"mapshaper {input_city_file} name='COMMUNE' -proj EPSG:4326 "
        "-filter \"'69123,13055,75056'.indexOf(INSEE_COM) > -1\" invert "
        '-each "INSEE_COG=INSEE_COM" '
        "-o force "
        f'{output} format={output_format} extension=".{output_format}" singles'
    )
    subprocess.run(cmd, shell=True, check=True, text=True)

    return output
