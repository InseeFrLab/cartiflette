#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from typing import List

from .utils import run


def mapshaper_dissolve(
    input_file: str,
    by: str,
    copy_fields: List[str] = None,
    calc: List[str] = None,
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
    quiet: bool = True,
) -> str:
    """
    Dissolve geometries

    Dissolve geometries on field `by`, keeping fields `copy_fields`. Other
    fields should be computaded using javascript functions with `calc`
    argument.


    Parameters
    ----------
    input_file : str
        Path to the input file.
    by : str
        Field used to dissolve
    copy_fields : List[str], optional
        Copies values from the first feature in each group of dissolved
        features. The default is None.
    calc : Listr[str], optional
        Fields on which computed should be operated, describing valid js
        functions. For instance ["POPULATION=sum(POPULATION)"]. The default
        is None.
    output_dir : str
        Directory to store the output file. The default is "temp"
    output_name : str, optional
        The path to write the file to (without extension).
        The default is "concatenated"
    output_format : str, optional
        The format to write the outputfile. The default is "geojson".
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

    name = "_".join(by)
    by = ",".join(by)
    cmd = (
        f"mapshaper {input_file} "
        f"name='{name}' "
        "-proj EPSG:4326 "
        f"-dissolve {by} "
    )
    if calc:
        calc = ",".join(calc)
        cmd += f"calc='{calc}' "
    if copy_fields:
        cmd += "copy-fields=" + ",".join(copy_fields)

    # fix_geo = "fix-geometry" if output_format == "topojson" else ""

    cmd += (
        f" {quiet}"
        f" -o {output} force "
        # f"{fix_geo}"
    )

    run(cmd)

    return output
