"""
Module's constants
"""

ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "production"
# First vintage published as consolidated GeoParquet files. From this vintage
# on, no GeoJSON file is published: the API serves GeoJSON from the GeoParquet.
# Earlier vintages may have GeoJSON files (2022, former pipeline).
PARQUET_FIRST_YEAR = 2025
