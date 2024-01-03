import os
import subprocess

from cartiflette.utils import DICT_CORRESP_IGN
from .mapshaper_wrangling import mapshaper_enrich, mapshaper_split


def mapshaperize_split(
    local_dir="temp",
    config_file_city={},
    format_output="topojson",
    niveau_polygons="COMMUNE",
    niveau_agreg="DEPARTEMENT",
    provider="IGN",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    territory="metropole",
    crs=4326,
    simplification=0,
    dict_corresp=DICT_CORRESP_IGN,
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

    # City level borders, file location
    directory_city = config_file_city.get("location", local_dir)
    initial_filename_city = config_file_city.get("filename", "COMMUNE")
    extension_initial_city = config_file_city.get("extension", "shp")

    output_path = (
        f"{local_dir}/{territory}/{niveau_agreg}/{format_output}/{simplification=}"
    )

    os.makedirs(output_path, exist_ok=True)

    if simplification_percent != 0:
        option_simplify = f"-simplify {simplification_percent}% "
    else:
        option_simplify = ""

    # STEP 1: ENRICHISSEMENT AVEC COG
    mapshaper_enrich(
        local_dir=directory_city,
        filename_initial=initial_filename_city,
        extension_initial=extension_initial_city,
        dict_corresp=dict_corresp,
        output_path="temp.geojson",
    )

    if niveau_polygons != initial_filename_city:
        csv_list_vars = (
            f"{dict_corresp[niveau_polygons]},"
            f"{dict_corresp[niveau_agreg]},"
            f"{dict_corresp['LIBELLE_' + niveau_polygons]}"
        )
        if niveau_agreg != "FRANCE_ENTIERE":
            csv_list_vars = f"{csv_list_vars},{dict_corresp['LIBELLE_' + niveau_agreg]}"

        # STEP 1B: DISSOLVE IF NEEDED
        cmd_dissolve = (
            f"mapshaper temp.geojson "
            f"name='' -proj EPSG:4326 "
            f"-dissolve {dict_corresp[niveau_polygons]} "
            f"calc='POPULATION=sum(POPULATION)' "
            f"copy-fields={csv_list_vars} "
            "-o temp.geojson force"
        )
        subprocess.run(cmd_dissolve, shell=True, check=True)

    # STEP 2: SPLIT ET SIMPLIFIE
    mapshaper_split(
        input_file="temp.geojson",
        layer_name="",
        split_variable=dict_corresp[niveau_agreg],
        output_path=output_path,
        format_output=format_output,
        crs=crs,
        option_simplify=option_simplify,
        source_identifier=f"{provider}:{source}",
    )

    return output_path


def mapshaperize_split_merge(
    format_output="topojson",
    niveau_agreg="DEPARTEMENT",
    provider="IGN",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    territory="metropole",
    config_file_city={},
    config_file_arrondissement={},
    local_dir="temp",
    crs=4326,
    simplification=0,
    dict_corresp=DICT_CORRESP_IGN,
):
    simplification_percent = simplification if simplification is not None else 0

    # City level borders, file location
    directory_city = config_file_city.get("location", local_dir)
    initial_filename_city = config_file_city.get("filename", "COMMUNE")
    extension_initial_city = config_file_city.get("extension", "shp")

    # Arrondissement level borders, file location
    directory_arrondissement = config_file_arrondissement.get("location", local_dir)
    initial_filename_arrondissement = config_file_arrondissement.get(
        "filename", "ARRONDISSEMENT_MUNICIPAL"
    )
    extension_initial_arrondissement = config_file_arrondissement.get(
        "extension", "shp"
    )

    # Intermediate output location
    output_path = (
        f"{local_dir}/{territory}/{niveau_agreg}/{format_output}/{simplification=}"
    )

    if simplification_percent != 0:
        option_simplify = f"-simplify {simplification_percent}% "
    else:
        option_simplify = ""

    format_intermediate = "geojson"

    # PREPROCESS CITIES
    file_city = f"{directory_city}/{initial_filename_city}.{extension_initial_city}"
    subprocess.run(
        (
            f"mapshaper {file_city} name='COMMUNE' "
            f"-proj EPSG:4326 "
            f"-filter '\"69123,13055,75056\".indexOf(INSEE_COM) > -1' invert "
            f'-each "INSEE_COG=INSEE_COM" '
            f"-o {output_path}/communes_simples.{format_intermediate} "
            f'format={format_intermediate} extension=".{format_intermediate}" singles'
        ),
        shell=True,
        check=True,
    )

    # PREPROCESS ARRONDISSEMENT
    file_arrondissement = (
        f"{directory_arrondissement}/"
        f"{initial_filename_arrondissement}.{extension_initial_arrondissement}"
    )
    subprocess.run(
        (
            f"mapshaper {file_arrondissement} "
            f"name='ARRONDISSEMENT_MUNICIPAL' "
            f"-proj EPSG:4326 "
            f"-rename-fields INSEE_COG=INSEE_ARM "
            f"-each 'STATUT=\"Arrondissement municipal\" ' "
            f"-o {output_path}/arrondissements.{format_intermediate} "
            f'format={format_intermediate} extension=".{format_intermediate}"'
        ),
        shell=True,
        check=True,
    )

    # MERGE CITIES AND ARRONDISSEMENT
    subprocess.run(
        (
            f"mapshaper "
            f"{output_path}/communes_simples.{format_intermediate} "
            f"{output_path}/arrondissements.{format_intermediate} snap combine-files "
            f"-proj EPSG:4326 "
            f"-rename-layers COMMUNE,ARRONDISSEMENT_MUNICIPAL "
            f"-merge-layers target=COMMUNE,ARRONDISSEMENT_MUNICIPAL force "
            f"-rename-layers COMMUNE_ARRONDISSEMENT "
            f"-o {output_path}/raw.{format_intermediate} "
            f'format={format_intermediate} extension=".{format_intermediate}"'
        ),
        shell=True,
        check=True,
    )

    # STEP 1: ENRICHISSEMENT AVEC COG
    mapshaper_enrich(
        local_dir=output_path,
        filename_initial="raw",
        extension_initial=format_intermediate,
        output_path=f"{output_path}/raw2.{format_intermediate}",
        dict_corresp=DICT_CORRESP_IGN,
    )

    # TRANSFORM AS NEEDED
    mapshaper_split(
        input_file=f"{output_path}/raw2.{format_intermediate}",
        layer_name="",
        split_variable=dict_corresp[niveau_agreg],
        output_path=output_path,
        format_output=format_output,
        crs=crs,
        option_simplify=option_simplify,
        source_identifier=f"{provider}:{source}",
    )

    return output_path
