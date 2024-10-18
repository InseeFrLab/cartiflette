#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from .utils import run


def mapshaper_capture_cities_from_ultramarine_territories(
    input_city_file: str,
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
) -> str:
    """
    Remove cities from departements, and keep only cities from ultramarine
    territories (Saint-Martin, etc.).

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
        "-filter \"'saint-barthelemy,saint-pierre-et-miquelon,saint-martin'"
        '.indexOf(AREA) > -1" '
        "-drop fields=TYP_IRIS "
        "-o force "
        f'{output} format={output_format} extension=".{output_format}" singles'
    )
    run(cmd)

    return output
