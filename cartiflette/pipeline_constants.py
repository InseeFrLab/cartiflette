# -*- coding: utf-8 -*-

from cartiflette.config import DATASETS_HIGH_RESOLUTION

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
        ["COMMUNE", "CANTON", "ARRONDISSEMENT", "DEPARTEMENT", "REGION"],
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
        "EPCI",
        "UNITE_URBAINE",
        "ZONE_EMPLOI",
        "BASSIN_VIE",
        "AIRE_ATTRACTION_VILLES",
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
        "EPCI",
        "UNITE_URBAINE",
        "ZONE_EMPLOI",
        "BASSIN_VIE",
        "AIRE_ATTRACTION_VILLES",
        "ARRONDISSEMENT",
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "CANTON": [
        "CANTON",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
}

# which territorial splits can be derived from a given geodataset (which
# borders' levels has been deduced from raw sources by dissolution)
AVAILABLE_TERRITORIAL_SPLITS_FOR_BORDERS = {
    # borders -> [filter_by1, filter_by2, ... ]
    "IRIS": [
        # "COMMUNE" -> too much files generated, trigger this only if usecase
        # CANTON -> if INSEE can prepare a junction between IRIS and CANTON
        "BASSIN_VIE",
        "ZONE_EMPLOI",
        "UNITE_URBAINE",
        "AIRE_ATTRACTION_VILLES",
        "ARRONDISSEMENT",
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
        "ARRONDISSEMENT",
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
        "ARRONDISSEMENT",
        "DEPARTEMENT",
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "EPCI": [
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

# Check integrity
all_dissolutions = {
    dissolution
    for key, dissolutions in AVAILABLE_DISSOLUTIONS_FROM_RAW_MESH.items()
    for dissolution in dissolutions
}
all_dissolutions = all_dissolutions

all_borders = {
    split
    for key, splits in AVAILABLE_TERRITORIAL_SPLITS_FOR_BORDERS.items()
    for split in splits
} | {
    # unwanted splits (too much files without due use case)
    "IRIS",  # -> will never need to make a map for a given IRIS
    "COMMUNE",  # -> should never need to make a map for a given COMMUNE
    "ARRONDISSEMENT_MUNICIPAL",  # -> should never need to make a map for a given ARM
    "CANTON",  # -> might need it ?
    "EPCI",  # -> might need it ?
}

differences = all_borders ^ all_dissolutions
if differences:
    raise ValueError(
        "keys of AVAILABLE_DISSOLUTIONS_FROM_RAW_MESH must be the same as "
        "every available dissolution from "
        "AVAILABLE_DISSOLUTIONS_FROM_RAW_MESH. Found the following "
        f"differences : {differences}"
    )
