# -*- coding: utf-8 -*-

from .config import DATASETS_HIGH_RESOLUTION

# Keys for COG_TERRITOIRE and IRIS are True for high resolution and False for
# low resolution of datasets
COG_TERRITOIRE = {
    False: "EXPRESS-COG-CARTO-TERRITOIRE",
    True: "EXPRESS-COG-TERRITOIRE",
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

PIPELINE_CRS = [4326]
PIPELINE_SIMPLIFICATION_LEVELS = [0, 40]
PIPELINE_FORMATS = ["geojson", "topojson", "gpkg"]

# which dissolutions can be operated from a given raw geodataset, depending
# of it's source (either from IRIS or from COMMUNES)
AVAILABLE_DISSOLUTIONS_FROM_RAW_MESH = {
    "IRIS": [
        "IRIS",
        "COMMUNE",
        "ARRONDISSEMENT_MUNICIPAL",
        "ARRONDISSEMENT",
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "COMMUNE": [
        "COMMUNE",
        "ARRONDISSEMENT_MUNICIPAL",
        "ARRONDISSEMENT",
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "CANTON": ["CANTON"],
}

# which territorial splits can be derived from a given geodataset (which
# borders' levels has been deduced from raw sources by dissolution)
AVAILABLE_TERRITORIAL_SPLITS_FOR_BORDERS = {
    # borders -> [filter_by1, filter_by2, ... ]
    "IRIS": [
        # "COMMUNE" -> two much files generated, trigger this only if usecase
        # CANTON -> if INSEE confirms this can be done?
        "BASSIN_VIE",
        "ZONE_EMPLOI",
        "UNITE_URBAINE",
        "AIRE_ATTRACTION_VILLES",
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "ARRONDISSEMENT_MUNICIPAL": [
        "BASSIN_VIE",
        "ZONE_EMPLOI",
        "UNITE_URBAINE",
        "AIRE_ATTRACTION_VILLES",
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "COMMUNE": [
        "BASSIN_VIE",
        "ZONE_EMPLOI",
        "UNITE_URBAINE",
        "AIRE_ATTRACTION_VILLES",
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "CANTON": [
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "ARRONDISSEMENT": [
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "DEPARTEMENT": [
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "REGION": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "BASSIN_VIE": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "ZONE_EMPLOI": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "UNITE_URBAINE": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "AIRE_ATTRACTION_VILLES": [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
}
