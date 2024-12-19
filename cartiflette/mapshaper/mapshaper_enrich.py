#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from typing import List

from .utils import run


def mapshaper_enrich(
    input_geodata_file: str,
    input_metadata_file: str,
    keys: List[str],
    dtype: str = None,
    drop: list = None,
    rename: dict = None,
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
    quiet: bool = True,
) -> str:
    """
    Enriches an initial geodata file with additional data using Mapshaper.

    Parameters
    ----------
    input_geodata_file : str
        Path to the input geodata file.
    input_metadata_file : str
        Path to the input metadata file to join to the geodata file.
    keys : List[str]
        List of fields used for joining the dataframes. Should be a tuple
        corresponding to left-field and right-field, for instance
        ['INSEE_COM', 'CODGEO']
    dtype : dict, optional
        Dtypes (among "str", "string", "num", "number"), for
        instance {"INSEE_REG": "str"} . Default is None.
    drop : list, optional
        List of columns to drop (if not None). Default is None.
    rename : dict, optional
        List of columns to rename (if not None) in a pandas' syntax-like.
        To rename A -> B, pass {"A": "B"}. The default is None.
    output_dir : str, optional
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
    dtype = ",".join(
        [f"{key}:{val}" for key, val in dtype.items()] if dtype else []
    )
    keys = ",".join(keys)
    drop = ",".join(drop if drop else [])

    # Warning : for mapshaper, to rename A -> B, use B=A syntax!
    rename = ",".join(
        [f"{val}={key}" for key, val in rename.items()] if rename else []
    )

    # Mapshaper command for the enrichment process
    cmd = (
        f"mapshaper {input_geodata_file} "
        "name='' -proj EPSG:4326 "
        f"-join {input_metadata_file} keys={keys} field-types={dtype} "
        f"-filter-fields {drop} invert "
        f"-rename-fields {rename} "
        "-each \"PAYS='France'\" "
        f"{quiet}"
        f"-o {output} force"
    )

    # Run Mapshaper command
    run(cmd)

    return output


# %%
