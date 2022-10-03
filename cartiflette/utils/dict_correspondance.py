def dict_corresp_decoupage():
    corresp_decoupage_columns = {
        "region": 'INSEE_REG',
        "departement": "INSEE_DEP",
        "commune": "INSEE_COM"
        }
    return corresp_decoupage_columns

def create_format_standardized():    
    format_standardized = {
        "geojson": 'geojson',
        "geopackage": "GPKG",
        "gpkg": "GPKG",
        "shp": "shp",
        "shapefile": "shp",
        "geoparquet": "parquet",
        "parquet": "parquet"
    }
    return format_standardized

def create_format_driver():
    gpd_driver = {
        "geojson": "GeoJSON",
        "GPKG": "GPKG",
        "shp": None,
        "parquet": None
    }
    return gpd_driver