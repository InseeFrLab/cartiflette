"""
Configuration of the production pipeline.

Reading and writing targets are deliberately distinct: the pipeline never
writes where the clients read from (``projet-cartiflette/production``)
unless this is explicitly (and knowingly) configured.
"""

import os

from cartiflette import __version__

ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

# Where the published files live (read by the clients)
PRODUCTION_BUCKET = "projet-cartiflette"
PRODUCTION_PATH = "production"

# Where the pipeline writes by default: a test location, namespaced by the
# pipeline version so that it never overwrites files of earlier tests.
WRITE_BUCKET = os.environ.get("CARTIFLETTE_WRITE_BUCKET", PRODUCTION_BUCKET)
WRITE_PATH = os.environ.get("CARTIFLETTE_WRITE_PATH", f"test/v{__version__}")

# Writing to the production prefix requires this variable to be set to
# exactly this value; anything else raises before any upload.
ALLOW_PRODUCTION_WRITE = (
    os.environ.get("CARTIFLETTE_ALLOW_PRODUCTION_WRITE") == "i-know-what-i-am-doing"
)

# Label used in the S3 paths and in the SOURCE field of the outputs. The
# IGN product is now ADMIN-EXPRESS-COG-CARTO "France entière", but the label is
# kept so that the paths read by the existing clients are unchanged.
PROVIDER = "IGN"
DATASET_FAMILY = "ADMINEXPRESS"
SOURCE = "EXPRESS-COG-CARTO-TERRITOIRE"
# Historical value of the `territory` path segment for every split file.
TERRITORY = "metropole"

# GeoJSON: one file per value of the split level. GeoParquet: one consolidated
# file per level and geometry (see cartiflette.paths.create_path_consolidated)
GEOMETRIES = ("FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES")
# Size of the row groups of the consolidated files (DuckDB minimum: 2048)
ROW_GROUP_SIZE = 2048
