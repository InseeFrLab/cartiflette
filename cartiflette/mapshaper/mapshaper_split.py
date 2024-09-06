#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import subprocess


def mapshaper_split(
    input_file: str = "temp.geojson",
    layer_name: str = "",
    split_variable: str = "DEPARTEMENT",
    output_dir: str = "temp",
    format_output: str = "geojson",
    crs: int = 4326,
    option_simplify: str = "",
    source_identifier: str = "",
) -> None:
    """
    Splits a GeoJSON file based on a specified variable using Mapshaper.

    Parameters:
    - input_file (str): The input GeoJSON file to be split (default is "temp.geojson").
    - layer_name (str): The name of the layer within the GeoJSON file (default is "").
    - split_variable (str): The variable used for splitting the GeoJSON file
      (default is "DEPARTEMENT").
    - output_dir (str): The dir for the output file file after splitting
      (default is "temp").
    - format_output (str): The format for the output GeoJSON file (default is "geojson").
    - crs (int): The coordinate reference system EPSG code (default is 4326).
    - option_simplify (str): Additional options for simplifying geometries (default is "").
    - source_identifier (str): Identifier for the data source (default is "").

    Returns:
    - None: The function runs Mapshaper with the specified commands and splits the GeoJSON file.
    """
    # Mapshaper command for the splitting process

    try:
        os.makedirs(output_dir)
    except FileExistsError:
        pass
    cmd = (
        f"mapshaper {input_file} name='{layer_name}' -proj EPSG:{crs} "
        f"{option_simplify}"
        f"-each \"SOURCE='{source_identifier}'\" "
        f"-split {split_variable} "
        f"-o '{output_dir}/' "
        f'format={format_output} extension=".{format_output}" singles'
    )

    # Run Mapshaper command
    subprocess.run(
        cmd,
        shell=True,
        check=True,
        text=True,
    )
