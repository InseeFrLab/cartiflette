import subprocess

DICT_CORRESP_IGN = {
    "REGION": "INSEE_REG",
    "DEPARTEMENT": "INSEE_DEP",
    "FRANCE_ENTIERE": "PAYS",
    "LIBELLE_REGION": "LIBELLE_REGION",
    "LIBELLE_DEPARTEMENT": "LIBELLE_DEPARTEMENT",
}


def mapshaperize_split(
    local_dir="temp",
    filename_initial="COMMUNE",
    extension_initial="shp",
    format_output="topojson",
    niveau_polygons="COMMUNE",
    niveau_agreg="DEPARTEMENT",
    provider="IGN",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022,
    dataset_family="ADMINEXPRESS",
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

    output_path = f"{local_dir}/{niveau_agreg}/{format_output}/{simplification=}"

    if simplification_percent != 0:
        option_simplify = f"-simplify {simplification_percent}% "
    else:
        option_simplify = ""

    # STEP 1: ENRICHISSEMENT AVEC COG
    cmd_step1 = (
        f"mapshaper {local_dir}/{filename_initial}.{extension_initial} "
        f"name='' -proj EPSG:4326 "
        f"-join temp/tagc.csv "
        f"keys=INSEE_COM,CODGEO field-types=INSEE_COM:str,CODGEO:str "
        f"-filter-fields INSEE_CAN,INSEE_ARR,SIREN_EPCI,DEP,REG invert "
        f"-each \"{dict_corresp['FRANCE_ENTIERE']}='France'\" "
        "-o temp.geojson"
    )

    subprocess.run(cmd_step1, shell=True, check=True)

    if niveau_polygons != filename_initial:
        csv_list_vars = (
            f"{dict_corresp[niveau_polygons]},"
            f"{dict_corresp[niveau_agreg]},"
            f"{dict_corresp['LIBELLE_' + niveau_polygons]}"
        )
        if niveau_agreg != "FRANCE_ENTIERE":
            csv_list_vars = f"{csv_list_vars},{dict_corresp['LIBELLE_' + niveau_agreg]}"

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
    cmd_step2 = (
        f"mapshaper temp.geojson name='' -proj EPSG:{crs} "
        f"{option_simplify}"
        f"-each \"SOURCE='{provider}:{source}'\" "
        f"-split {dict_corresp[niveau_agreg]} "
        f'-o {output_path} format={format_output} extension=".{format_output}" singles'
    )

    subprocess.run(cmd_step2, shell=True, check=True)

    return output_path


def mapshaperize_split_merge(
    local_dir="temp",
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
    dict_corresp=DICT_CORRESP_IGN,
):
    simplification_percent = simplification if simplification is not None else 0

    output_path = f"{local_dir}/{niveau_agreg}/{format_output}/{simplification=}"

    if simplification_percent != 0:
        option_simplify = f"-simplify {simplification_percent}% "
    else:
        option_simplify = ""

    format_intermediate = "geojson"

    # PREPROCESS CITIES
    subprocess.run(
        (
            f"mapshaper {local_dir}/COMMUNE.{extension_initial} name='COMMUNE' "
            f"-proj EPSG:4326 "
            f"-filter '\"69123,13055,75056\".indexOf(INSEE_COM) > -1' invert "
            f'-each "INSEE_COG=INSEE_COM" '
            f"-o {output_path}/communes_simples.{format_intermediate} "
            f'format={format_intermediate} extension=".{format_intermediate}" singles'
        ),
        shell=True,
        check=True
    )

    # PREPROCESS ARRONDISSEMENT
    subprocess.run(
        (
            f"mapshaper {local_dir}/ARRONDISSEMENT_MUNICIPAL.{extension_initial} "
            f"name='ARRONDISSEMENT_MUNICIPAL' "
            f"-proj EPSG:4326 "
            f"-rename-fields INSEE_COG=INSEE_ARM "
            f"-each 'STATUT=\"Arrondissement municipal\" ' "
            f"-o {output_path}/arrondissements.{format_intermediate} "
            f'format={format_intermediate} extension=".{format_intermediate}"'
        ),
        shell=True,
        check=True
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
        check=True
    )

    # STEP 1: ENRICHISSEMENT AVEC COG
    cmd_step1 = (
        f"mapshaper {output_path}/raw.{format_intermediate} "
        f"-join temp/tagc.csv "
        f"keys=INSEE_COM,CODGEO field-types=INSEE_COM:str,CODGEO:str "
        "-filter-fields INSEE_CAN,INSEE_ARR,SIREN_EPCI,INSEE_DEP,INSEE_REG invert "
        f"-rename-fields INSEE_DEP=DEP,INSEE_REG=REG "
        f"-o {output_path}/raw2.{format_intermediate}"
    )

    subprocess.run(cmd_step1, shell=True, check=True)

    # TRANSFORM AS NEEDED
    cmd_step2 = (
        f"mapshaper {output_path}/raw2.{format_intermediate} "
        f"{option_simplify}"
        f"-proj EPSG:{crs} "
        f"-each \"SOURCE='{provider}:{source}'\" "
        f"-split {dict_corresp[niveau_agreg]} "
        f'-o {output_path} format={format_output} extension=".{format_output}" singles'
    )

    subprocess.run(cmd_step2, shell=True, check=True)

    return output_path
