#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

from .utils import run


def mapshaper_add_field(
    input_file: str,
    label: str,
    value: str,
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
    quiet: bool = True,
) -> str:
    """
    Add a field (= a column/attribute) to the dataset.
    To add a static value, please set it between double+singles quotes,
    ("'blah'" for instance). To add a dynamic field (from existing fields), use
    a single double quote string (for isntance "INSEE_DEP+INSEE_CAN").

    Parameters
    ----------
    input_file : str
        Path to the input file.
    label : str
        The added field's name.
    value : str
        The value of the added field.
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

    output = f"{output_dir}/{output_name}.{output_format}"
    quiet = "-quiet " if quiet else " "

    cmd = (
        f"mapshaper {input_file} "
        f'-each "{label}={value}"'
        f"{quiet}"
        "-proj EPSG:4326 "
        f" -o {output}  "
        f'format={output_format} extension=".{output_format}" force'
    )

    # Run Mapshaper command
    run(cmd)

    return output
