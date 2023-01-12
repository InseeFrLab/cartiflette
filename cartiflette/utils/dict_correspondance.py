"""
Collection of utils to reformat inputs
"""


def dict_corresp_decoupage() -> dict:
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
        "france_entiere": "territoire"
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
    }
    return format_standardized


def create_format_driver() -> dict:
    """Transforms user-prompted format into Geopandas driver

    Returns:
        dict: Geopandas driver as well as user-prompted
         format
    """
    gpd_driver = {"geojson": "GeoJSON", "GPKG": "GPKG", "shp": None, "parquet": None}
    return gpd_driver


def official_epsg_codes() -> dict:
    crs_list = {
        "metropole": 2154,
        "martinique": 5490,
        "reunion": 2975,
        "guadeloupe": 5490,
        "guyane": 2972
        }
    return crs_list