# -*- coding: utf-8 -*-

from cartiflette.utils import (
    create_format_standardized,
    create_format_driver,
)


def _vectorfile_format_config(vectorfile_format: str) -> tuple[str, str]:
    """
    Returns a tuple of file extension and geopandas driver

    Parameters
    ----------
    vectorfile_format : str
        The format of the vector file, can be "geojson", "topojson",
        "geopackage", "gpkg", "shp", "shapefile", "geoparquet" or "parquet"

    Returns
    -------
    tuple
    * the extension of the file (resp. geojson, topojson, parquet, "GPKG",
      "GPKG", "shp", "shp", "parquet", "parquet")
    * the driver used by geopandas to process such files (resp. "GeoJSON",
      None, "GPKG", "GPKG", None, None, None, None)
    """

    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_write = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_write]

    return format_write, driver
