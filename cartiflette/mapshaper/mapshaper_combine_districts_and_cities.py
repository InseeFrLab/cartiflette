# -*- coding: utf-8 -*-

import os

from .utils import run


def mapshaper_combine_districts_and_cities(
    input_city_file: str,
    input_communal_districts_file: str,
    output_dir: str,
    output_name: str = "output",
    output_format: str = "geojson",
) -> str:
    """
    Combine cities' dataset with communal districts', ensure layer renamming
    before merging.

    Parameters
    ----------
    input_communal_districts_file : str
        Path to the input file.
    output_dir : str
        Directory to store the output file.
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

    # fix_geo = "fix-geometry" if output_format == "topojson" else ""
    output = f"{output_dir}/{output_name}.{output_format}"

    cmd = (
        f"mapshaper {input_city_file} {input_communal_districts_file} "
        "snap combine-files "
        "-proj EPSG:4326 "
        "-rename-layers COMMUNE,ARRONDISSEMENT_MUNICIPAL "
        "-merge-layers target=COMMUNE,ARRONDISSEMENT_MUNICIPAL force "
        "-rename-layers COMMUNE_ARRONDISSEMENT "
        f"-o {output} "
        # f"{fix_geo} "
        f"format={output_format} "
        f'extension=".{output_format}"'
    )
    run(cmd)

    return output
