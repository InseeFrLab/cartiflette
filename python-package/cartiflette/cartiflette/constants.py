"""
Module's constants
"""

import platformdirs

APP_NAME = "cartiflette"
DIR_CACHE = platformdirs.user_cache_dir(APP_NAME, ensure_exists=True)
CACHE_NAME = "cartiflette_http_cache.sqlite"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "production"
