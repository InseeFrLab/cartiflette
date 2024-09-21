"""
Collection of utils to reformat inputs
"""

import re

# TODO : rename and explicit purpose of constant!
DICT_CORRESP_ADMINEXPRESS = {
    "IRIS": re.compile("IRIS"),
    "REGION": re.compile("INSEE_REG"),
    "DEPARTEMENT": re.compile("INSEE_DEP"),
    "ARRONDISSEMENT": re.compile("INSEE_ARR"),
    "FRANCE_ENTIERE": re.compile("PAYS"),
    "FRANCE_ENTIERE_DROM_RAPPROCHES": re.compile("PAYS"),
    "LIBELLE_REGION": re.compile("LIBELLE_REGION"),
    "LIBELLE_DEPARTEMENT": re.compile("LIBELLE_DEPARTEMENT"),
    "BASSIN_VIE": re.compile("BV[0-9]{4}"),
    "AIRE_ATTRACTION_VILLES": re.compile("AAV[0-9]{4}"),
    "UNITE_URBAINE": re.compile("UU[0-9]{4}"),
    "ZONE_EMPLOI": re.compile("ZE[0-9]{4}"),
    "TERRITOIRE": re.compile("AREA"),
    "EPCI": re.compile("EPCI"),
    "LIBELLE_EPCI": re.compile("LIBELLE_EPCI"),
    "EPT": re.compile("EPT"),
    "LIBELLE_EPT": re.compile("LIBELLE_EPT"),
    "COMMUNE": re.compile("INSEE_COM"),
    "LIBELLE_COMMUNE": re.compile("LIBELLE_COMMUNE"),
    "LIBELLE_CANTON": re.compile("LIBELLE_CANTON"),
    "CANTON": re.compile("INSEE_CAN"),
    "ARRONDISSEMENT_MUNICIPAL": re.compile("INSEE_COMMUNE"),
    "LIBELLE_ARRONDISSEMENT_MUNICIPAL": re.compile("LIBELLE_COMMUNE"),
    # TODO: code SIREN ?
}


def dict_corresp_filter_by() -> dict:
    """Transforms explicit administrative borders into relevant column

    Returns:
        dict: Relevant column as well as initial
            user prompted administrative level
    """
    corresp_decoupage_columns = {
        "region": "INSEE_REG",
        "departement": "INSEE_DEP",
        "commune": "INSEE_COM",
        "commune_arrondissement": "INSEE_COM",
        "region_arrondissement": "INSEE_REG",
        "departement_arrondissement": "INSEE_DEP",
        "france_entiere": "territoire",
    }
    return corresp_decoupage_columns


def create_format_standardized() -> dict:
    """Transforms user-prompted format into geopandas format

    Returns:
        dict: Geopandas format as well as user-prompted
         format
    """
    format_standardized = {
        "geojson": "geojson",
        "geopackage": "GPKG",
        "gpkg": "GPKG",
        "shp": "shp",
        "shapefile": "shp",
        "geoparquet": "parquet",
        "parquet": "parquet",
        "topojson": "topojson",
    }
    return format_standardized


def create_format_driver() -> dict:
    """Transforms user-prompted format into Geopandas driver

    Returns:
        dict: Geopandas driver as well as user-prompted
         format
    """
    gpd_driver = {
        "geojson": "GeoJSON",
        "GPKG": "GPKG",
        "shp": None,
        "parquet": None,
        "topojson": None,
    }
    return gpd_driver


def official_epsg_codes() -> dict:
    crs_list = {
        "metropole": 2154,
        "martinique": 5490,
        "reunion": 2975,
        "guadeloupe": 5490,
        "guyane": 2972,
        "mayotte": 4326,
        "france_entiere": None,
    }
    return crs_list
