#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from .utils import run


def mapshaper_process_communal_districts(
    input_communal_districts_file: str,
    output_dir: str,
    output_name: str = "output",
    output_format: str = "geojson",
    quiet: bool = True,
) -> str:
    """
    Preprocess communal districts files to ensure

    Parameters
    ----------
    input_communal_districts_file : str
        Path to the input file.
    output_dir : str
        Directory to store the output file. The default is "temp".
    output_name : str, optional
        Name of the written file, without extension. The default is "output".
    output_format : str, optional
        Format for output file. The default is "geojson".
    quiet : bool, optional
        If True, inhibits console messages. The default is True.

    Returns
    -------
    output : str
        Path of the created file

    """
    try:
        os.makedirs(output_dir)
    except FileExistsError:
        pass

    quiet = "-quiet " if quiet else " "
    output = f"{output_dir}/{output_name}.{output_format}"

    cmd = (
        f"mapshaper {input_communal_districts_file} "
        "name='ARRONDISSEMENT_MUNICIPAL' "
        "-proj EPSG:4326 "
        "-rename-fields INSEE_COG=INSEE_ARM "
        "-each 'STATUT=\"Arrondissement municipal\"' "
        f"{quiet}"
        "-o force "
        f'{output} format={output_format} extension=".{output_format}"'
    )
    run(cmd)

    return output
