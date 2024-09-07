import os
import subprocess


def mapshaper_convert_mercator(
    input_file: str,
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
    filter_by: str = "",
) -> str:
    """
    Project a file to mercator.
    If identifier is given, will filter the file based on the following
    criteria: AREA='{identifier}'

    Parameters
    ----------
    input_file : str
        Path to the input file.
    output_dir : str
        Directory to store the output file. The default is "temp"
    output_name : str, optional
        The path to write the file to (without extension).
        The default is "concatenated"
    output_format : str, optional
        The format to write the outputfile. The default is "geojson".
    filter_by: str, optional
        The criteria to filter the input file, based on AREA field. The default
        is "", which will not perform any filter.

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

    if filter_by != "":
        filter_by = f"-each \"AREA='{filter_by}'\" "

    subprocess.run(
        (
            f"mapshaper {input_file} name='COMMUNE' "
            "-proj EPSG:4326 "
            f"{filter_by}"
            f"-o {output} force "
            f'format={output_format} extension=".{output_format}" singles'
        ),
        shell=True,
        check=True,
        text=True,
    )

    return output_name
