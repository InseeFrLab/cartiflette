#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from glob import glob
import os
import subprocess
from typing import List


def mapshaper_split(
    input_file: str,
    layer_name: str = "",
    split_variable: str = "DEPARTEMENT",
    output_dir: str = "temp",
    output_format: str = "geojson",
    crs: int = 4326,
    option_simplify: str = "",
    source_identifier: str = "",
) -> List[str]:
    """
    Splits a GeoJSON file based on a specified variable using Mapshaper.

    Parameters
    ----------
    input_file : str
        The input file to be split (default is "temp.geojson").
    layer_name : str, optional
        The name of the layer within the file. The default is "".
    split_variable : str, optional
        The variable used for splitting the file. The default is "DEPARTEMENT".
    output_dir : str, optional
        Directory to store output files. The default is "temp".
    output_format : str, optional
        Format for output files. The default is "geojson".
    crs : int, optional
        The coordinate reference system EPSG code. The default is 4326.
    option_simplify : str, optional
        Additional options for simplifying geometries, for instance
        "-simplify 50%". The default is "".
    source_identifier : str, optional
        Identifier for the data source. The default is "".

    Returns
    -------
    final_files : List[str]
        List of paths of created files

    """

    # make a temporary inner directory to retrieve the full list of produced
    # files at the end
    temp_output_dir = os.path.join(output_dir, "this_is_a_dumb_temp_directory")
    try:
        os.makedirs(temp_output_dir)
    except FileExistsError:
        pass

    # Mapshaper command for the splitting process
    cmd = (
        f"mapshaper {input_file} name='{layer_name}' -proj EPSG:{crs} "
        f"{option_simplify} "
        f"-each \"SOURCE='{source_identifier}'\" "
        f"-split {split_variable} "
        f"-o '{temp_output_dir}/' "
        f'format={output_format} extension=".{output_format}" singles'
    )

    # Run Mapshaper command
    subprocess.run(
        cmd,
        shell=True,
        check=True,
        text=True,
    )

    produced_files = glob(os.path.join(temp_output_dir, f"*.{output_format}"))
    final_files = [
        file.replace(temp_output_dir, output_dir) for file in produced_files
    ]
    [os.replace(src, dst) for src, dst in zip(produced_files, final_files)]

    return final_files