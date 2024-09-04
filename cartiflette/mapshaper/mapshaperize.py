import subprocess

from cartiflette.utils import DICT_CORRESP_ADMINEXPRESS
from .mapshaper_wrangling import mapshaper_enrich, mapshaper_split
from .mapshaper_closer import mapshaper_bring_closer


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
    dict_corresp=DICT_CORRESP_ADMINEXPRESS,
):
    simplification_percent = (
        simplification if simplification is not None else 0
    )

    # City level borders, file location
    directory_city = config_file_city.get("location", local_dir)
    initial_filename_city = config_file_city.get("filename", "COMMUNE")
    extension_initial_city = config_file_city.get("extension", "shp")

    # Arrondissement level borders, file location
    directory_arrondissement = config_file_arrondissement.get(
        "location", local_dir
    )
    initial_filename_arrondissement = config_file_arrondissement.get(
        "filename", "ARRONDISSEMENT_MUNICIPAL"
    )
    extension_initial_arrondissement = config_file_arrondissement.get(
        "extension", "shp"
    )

    # Intermediate output location
    output_path = f"{local_dir}/{territory}/{niveau_agreg}/{format_output}/{simplification=}"

    if simplification_percent != 0:
        option_simplify = f"-simplify {simplification_percent}% "
    else:
        option_simplify = ""

    format_intermediate = "geojson"

    # PREPROCESS CITIES
    file_city = (
        f"{directory_city}/{initial_filename_city}.{extension_initial_city}"
    )
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
        text=True,
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
        text=True,
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
        text=True,
    )

    # STEP 1: ENRICHISSEMENT AVEC COG
    mapshaper_enrich(
        local_dir=output_path,
        filename_initial="raw",
        extension_initial=format_intermediate,
        output_path=f"{output_path}/raw2.{format_intermediate}",
        dict_corresp=DICT_CORRESP_ADMINEXPRESS,
    )

    input_path = f"{output_path}/raw2.{format_intermediate}"

    if niveau_agreg.upper() == "FRANCE_ENTIERE_DROM_RAPPROCHES":
        input_path = mapshaper_bring_closer(input_path)

    # TRANSFORM AS NEEDED
    mapshaper_split(
        input_file=input_path,
        layer_name="",
        split_variable=dict_corresp[niveau_agreg],
        output_path=output_path,
        format_output=format_output,
        crs=crs,
        option_simplify=option_simplify,
        source_identifier=f"{provider}:{source}",
    )

    return output_path
