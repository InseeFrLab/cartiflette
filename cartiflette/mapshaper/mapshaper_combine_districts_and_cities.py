# -*- coding: utf-8 -*-

import os
import subprocess


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

    output = f"{output_dir}/{output_name}.{output_format}"

    subprocess.run(
        (
            f"mapshaper {input_city_file} {input_communal_districts_file} "
            "snap combine-files "
            "-proj EPSG:4326 "
            "-rename-layers COMMUNE,ARRONDISSEMENT_MUNICIPAL "
            "-merge-layers target=COMMUNE,ARRONDISSEMENT_MUNICIPAL force "
            "-rename-layers COMMUNE_ARRONDISSEMENT "
            f'-o {output} format={output_format} extension=".{output_format}"'
        ),
        shell=True,
        check=True,
        text=True,
    )

    return output
