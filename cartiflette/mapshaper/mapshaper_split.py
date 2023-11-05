import subprocess



DICT_CORRESP_IGN = {"REGION": "INSEE_REG", "DEPARTEMENT": "INSEE_DEP"}


def mapshaperize_split(
    local_dir="temp",
    filename_initial="COMMUNE",
    extension_initial="shp",
    format_output="topojson",
    niveau_agreg="DEPARTEMENT",
    provider="IGN",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022,
    dataset_family="ADMINEXPRESS",
    territory="metropole",
    crs=4326,
    simplification=0,
    dict_corresp=DICT_CORRESP_IGN
):
    """
    Processes shapefiles and splits them based on specified parameters using Mapshaper.

    Parameters
    ----------
    local_dir : str, optional
        The local directory for file storage, by default "temp".
    filename_initial : str, optional
        The initial filename, by default "COMMUNE".
    extension_initial : str, optional
        The initial file extension, by default "shp".
    format_output : str, optional
        The output format, by default "topojson".
    niveau_agreg : str, optional
        The level of aggregation for the split, by default "DEPARTEMENT".
    provider : str, optional
        The data provider, by default "IGN".
    source : str, optional
        The data source, by default "EXPRESS-COG-CARTO-TERRITOIRE".
    year : int, optional
        The year of the data, by default 2022.
    dataset_family : str, optional
        The dataset family, by default "ADMINEXPRESS".
    territory : str, optional
        The territory of the data, by default "metropole".
    crs : int, optional
        The coordinate reference system (CRS) code, by default 4326.
    simplification : int, optional
        The degree of simplification, by default 0.
    dict_corresp: dict
        A dictionary giving correspondance between niveau_agreg argument
        and variable names.

    Returns
    -------
    str
        The output path of the processed and split shapefiles.

    """

    simplification_percent = simplification if simplification is not None else 0

    output_path = f"{local_dir}/{niveau_agreg}/{format_output}/{simplification=}"

    if simplification_percent != 0:
        option_simplify = f"-simplify {simplification_percent}% "
    else:
        option_simplify = ""

    cmd = (
            f"mapshaper {local_dir}/{filename_initial}.{extension_initial} name='' -proj EPSG:{crs} "
            f"{option_simplify}"
            f"-each \"SOURCE='{provider}:{source}'\" "
            f"-split {dict_corresp[niveau_agreg]} "
            f"-o {output_path} format={format_output} extension=\".{format_output}\" singles"
        )


    subprocess.run(
        cmd,
        shell=True
    )

    return output_path


