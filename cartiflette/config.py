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

RETRYING = True
LEAVE_TQDM = False

# PIPELINE_DOWNLOAD_ARGS = {
#     "ADMIN-EXPRESS": [
#         "IGN",
#         "ADMINEXPRESS",
#         "EXPRESS-COG-TERRITOIRE",
#         [
#             "guadeloupe",
#             "martinique",
#             "guyane",
#             "reunion",
#             "mayotte",
#             "metropole",
#         ],
#     ],
#     "BDTOPO": ["IGN", "BDTOPO", "ROOT", "france_entiere"],
#     "IRIS": ["IGN", "CONTOUR-IRIS", "ROOT", None],
#     "COG": [
#         "Insee",
#         "COG",
#         [
#             "COMMUNE",
#             "CANTON",
#             "ARRONDISSEMENT",
#             "DEPARTEMENT",
#             "REGION",
#             "COLLECTIVITE",
#             "PAYS",
#         ],
#         "france_entiere",
#     ],
#     "BV 2022": ["Insee", "BV", "FondsDeCarte_BV_2022", "france_entiere"],
# }

# =============================================================================
# PIPELINE CONFIG
# =============================================================================

PIPELINE_DOWNLOAD_ARGS = {
    # "ADMIN-EXPRESS": [
    #     "IGN",
    #     "ADMINEXPRESS",
    #     "EXPRESS-COG-CARTO-TERRITOIRE",
    #     [
    #         "guadeloupe",
    #         "martinique",
    #         "guyane",
    #         "reunion",
    #         "mayotte",
    #         "metropole",
    #     ],
    # ],
    "IRIS-GE": [
        "IGN",
        "CONTOUR-IRIS",
        "CONTOUR-IRIS-TERRITOIRE",
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
    # # "IRIS-GE": [
    # #     "IGN",
    # #     "IRIS-GE",
    # #     "IRIS-GE-TERRITOIRE",
    # #     [
    # #         "guadeloupe",
    # #         "martinique",
    # #         "guyane",
    # #         "reunion",
    # #         "mayotte",
    # #         "metropole",
    # #         "saint-pierre-et-miquelon",
    # #         "saint-barthelemy",
    # #         "saint-martin",
    # #     ],
    # # ],
    # "COG": [
    #     "Insee",
    #     "COG",
    #     ["CANTON", "ARRONDISSEMENT", "DEPARTEMENT", "REGION"],
    #     "france_entiere",
    # ],
    # "TAGC": ["Insee", "TAGC", "APPARTENANCE"],
    # "TAGIRIS": ["Insee", "TAGIRIS", "APPARTENANCE"],
}

PIPELINE_SIMPLIFICATION_LEVELS = [0, 40]
