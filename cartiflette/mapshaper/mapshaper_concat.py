#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import subprocess


def mapshaper_concat(
    input_dir: str,
    output_file: str = "concatenated",
    input_format: str = "*",
    output_format: str = "geojson",
):
    """
    Concat multiple files (all files should have the same projection).

    Parameters
    ----------
    input_dir : str
        Directory containing the files to concat
    output_file : str, optional
        The path to write the file to (without extension).
        The default is "concatenated"
    input_format : str, optional
        Input file's format. If "*", will match every kind of files.
        The default is "*"
    output_format : str, optional
        The format to write the outputfile. The default is "geojson".

    Returns
    -------
    None.

    """
    subprocess.run(
        (
            f"mapshaper -i {input_dir}/**/*.{input_format}"
            " combine-files name='COMMUNE' "
            f"-proj EPSG:4326 "
            f"-merge-layers "
            f"-o {output_file}.{output_format} "
            f"format={output_format} "
            f'extension=".{output_format}" singles'
        ),
        shell=True,
        check=True,
        text=True,
    )
