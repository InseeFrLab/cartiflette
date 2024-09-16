# -*- coding: utf-8 -*-
import os
import warnings
from dotenv import load_dotenv
import s3fs

load_dotenv(override=True)

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "test"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

kwargs = {}
for key in ["token", "secret", "key"]:
    try:
        kwargs[key] = os.environ[key]
    except KeyError:
        continue
FS = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL}, **kwargs)
# Double the standard timeouts
FS.read_timeout = 30
FS.connect_timeout = 10

THREADS_DOWNLOAD = 5
# Nota : each thread may also span the same number of children threads;
# set to 1 for debugging purposes (will deactivate multithreading)

RETRYING = True  # WHETHER TO USE RETRYING MODULE ON DOWNLOAD/UPLOAD

# =============================================================================
# PIPELINE CONFIG
# =============================================================================

# set to low resolution datasets for dev environment, high for anything else
INTERMEDIATE_FORMAT = "geojson"
DATASETS_HIGH_RESOLUTION = os.environ.get("ENVIRONMENT", "dev") != "dev"

if not DATASETS_HIGH_RESOLUTION:
    warnings.warn(
        "cartiflette is running with dev configuration, using only low "
        "resolution datasets"
    )
