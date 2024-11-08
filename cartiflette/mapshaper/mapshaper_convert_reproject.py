import os

from .utils import run


def mapshaper_convert_reproject(
    input_file: str,
    epsg: int = 4326,
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
    filter_by: str = "",
    quiet: bool = True,
) -> str:
    """
    Project a file to a given EPSG (into a given format).
    If identifier is given, will filter the file based on the following
    criteria: AREA='{identifier}'

    Parameters
    ----------
    input_file : str
        Path to the input file.
    epsg : int, optional
        EPSG code to project into. The default is 4326.
    output_dir : str, optional
        Directory to store the output file. The default is "temp"
    output_name : str, optional
        The path to write the file to (without extension).
        The default is "concatenated"
    output_format : str, optional
        The format to write the outputfile. The default is "geojson".
    filter_by: str, optional
        The criteria to filter the input file, based on AREA field. The default
        is "", which will not perform any filter.
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

    if filter_by != "":
        filter_by = f"-each \"AREA='{filter_by}'\" "

    cmd = (
        f"mapshaper {input_file} name='{output_name}' "
        f"-proj EPSG:{epsg} "
        f"{filter_by}"
        f"{quiet}"
        f"-o {output} force "
        f'format={output_format} extension=".{output_format}" singles'
    )

    run(cmd)

    return output
