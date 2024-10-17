#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from .utils import run


def mapshaper_simplify(
    input_file: str,
    option_simplify: str = "",
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
) -> str:
    """
    SImplify geometries


    Parameters
    ----------
    input_file : str
        Path to the input file.
    option_simplify : str, optional
        Additional options for simplifying geometries, for instance
        "-simplify 50%". The default is "".
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

    # fix_geo = "fix-geometry" if output_format == "topojson" else ""

    output = f"{output_dir}/{output_name}.{output_format}"

    cmd = (
        f"mapshaper {input_file} "
        "-proj EPSG:4326 "
        f"{option_simplify} "
        f" -o {output} force "
        # f"{fix_geo}"
    )

    run(cmd)

    return output
