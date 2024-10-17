#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from .utils import run


def mapshaper_concat(
    input_dir: str,
    input_format: str = "*",
    output_dir: str = "temp",
    output_name: str = "concatenated",
    output_format: str = "geojson",
) -> str:
    """
    Concat multiple files (all files should have the same projection).

    Parameters
    ----------
    input_dir : str
        Directory containing the files to concat
    input_format : str, optional
        Input file's format. If "*", will match every kind of files.
        The default is "*"
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

    cmd = (
        f"mapshaper -i {input_dir}/*.{input_format}"
        f" combine-files name='{output_name}' "
        f"-proj EPSG:4326 "
        f"-merge-layers "
        f'-o {output} format={output_format} extension=".{output_format}" '
        "singles"
    )

    run(cmd)

    return output
