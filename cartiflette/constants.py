# -*- coding: utf-8 -*-

import geopandas as gpd
import logging
from shapely.geometry import box


logger = logging.getLogger(__name__)

REFERENCES = [
    # use : https://boundingbox.klokantech.com/
    {"location": "metropole", "geometry": box(-5.45, 41.26, 9.83, 51.31)},
    {"location": "guyane", "geometry": box(-54.6, 2.11, -51.5, 5.98)},
    {
        "location": "martinique",
        "geometry": box(-61.4355, 14.2217, -60.6023, 15.0795),
    },
    {
        "location": "guadeloupe",
        "geometry": box(-62.018, 15.6444, -60.792, 16.714),
    },
    {
        "location": "reunion",
        "geometry": box(55.0033, -21.5904, 56.0508, -20.6728),
    },
    {
        "location": "mayotte",
        "geometry": box(44.7437, -13.2733, 45.507, -12.379),
    },
    {
        "location": "saint_pierre_et_miquelon",
        "geometry": box(-56.6975, 46.5488, -55.9066, 47.3416),
    },
]

REFERENCES = gpd.GeoDataFrame(REFERENCES, crs=4326)

DOWNLOAD_PIPELINE_ARGS = {
    "ADMIN-EXPRESS": [
        "IGN",
        "ADMINEXPRESS",
        "EXPRESS-COG-TERRITOIRE",
        [
            "guadeloupe",
            "martinique",
            "guyane",
            "reunion",
            "mayotte",
            "metropole",
        ],
    ],
    "BDTOPO": ["IGN", "BDTOPO", "ROOT", "france_entiere"],
    "IRIS": ["IGN", "CONTOUR-IRIS", "ROOT", None],
    "COG": [
        "Insee",
        "COG",
        [
            "COMMUNE",
            "CANTON",
            "ARRONDISSEMENT",
            "DEPARTEMENT",
            "REGION",
            "COLLECTIVITE",
            "PAYS",
        ],
        "france_entiere",
    ],
    "BV 2022": ["Insee", "BV", "FondsDeCarte_BV_2022", "france_entiere"],
}

# EXPRESS-COG ?
# EXPRESS-COG-CARTO-TERRITOIRE ?
# EXPRESS-COG-CARTO ?
