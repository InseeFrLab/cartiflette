# -*- coding: utf-8 -*-
import os
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
DATASETS_HIGH_RESOLUTION = False
COG_TERRITOIRE = {
    # Keys are DATASETS_HIGH_RESOLUTION's potential value
    False: "EXPRESS-COG-CARTO-TERRITOIRE",
    True: "EXPRESS-COG-CARTO-TERRITOIRE",
}
IRIS = {
    # Keys are DATASETS_HIGH_RESOLUTION's potential value
    False: "CONTOUR-IRIS",
    True: "IRIS-GE",
}

PIPELINE_DOWNLOAD_ARGS = {
    "ADMIN-EXPRESS": [
        "IGN",
        "ADMINEXPRESS",
        COG_TERRITOIRE[DATASETS_HIGH_RESOLUTION],
        [
            "guadeloupe",
            "martinique",
            "guyane",
            "reunion",
            "mayotte",
            "metropole",
        ],
    ],
    "IRIS": [
        "IGN",
        "IRIS",
        IRIS[DATASETS_HIGH_RESOLUTION],
        [
            "guadeloupe",
            "martinique",
            "guyane",
            "reunion",
            "mayotte",
            "metropole",
            "saint-pierre-et-miquelon",
            "saint-barthelemy",
            "saint-martin",
        ],
    ],
    "COG": [
        "Insee",
        "COG",
        ["CANTON", "ARRONDISSEMENT", "DEPARTEMENT", "REGION"],
        "france_entiere",
    ],
    "TAGC": ["Insee", "TAGC", "APPARTENANCE"],
    "TAGIRIS": ["Insee", "TAGIRIS", "APPARTENANCE"],
}

PIPELINE_SIMPLIFICATION_LEVELS = [0, 40]
