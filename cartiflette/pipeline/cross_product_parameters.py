import logging

import pandas as pd

from cartiflette.pipeline_constants import (
    AVAILABLE_DISSOLUTIONS_FROM_RAW_MESH,
    AVAILABLE_TERRITORIAL_SPLITS_FOR_BORDERS,
    PIPELINE_DOWNLOAD_ARGS,
    PIPELINE_SIMPLIFICATION_LEVELS,
)

logger = logging.getLogger(__name__)


def flatten_dict_to_list(dict_with_list: dict) -> list:
    """
    Restructures a nested dictionary by flattening its values and their
    corresponding keys.

    Parameters:
    -----------
    dict_with_list : dict
        A dictionary with list values to be restructured.

    Returns:
    --------
    flattened_list : list
        A list of lists containing key-value pairs obtained by flattening the
        input dictionary.

    Example:
    --------
    Example usage:
        sample_dict = {'a': [1, 2, 3], 'b': [4, 5]}
        result = flatten_dict(sample_dict)
        print(result)

    This will output:
        [['a', 1], ['a', 2], ['a', 3], ['b', 4], ['b', 5]]
    """
    flattened_list = [
        [key, inner_value]
        for key, values in dict_with_list.items()
        for inner_value in values
    ]

    return flattened_list


def crossproduct_parameters_production(
    list_format: list,
    years: list,
    crs_list: list,
    simplifications: list = PIPELINE_SIMPLIFICATION_LEVELS,
) -> list:
    """
    Generates a dict of arguments commanding the generation of output
    geodatasets.

    Note that the length of the dict represents the number of downstream pods,
    but that the number of combinations does NOT correspond to the number of
    generated geodatasets :
    * first, there may be two different candidates for the same geodataset:
      cities may be derived from raw COMMUNE geodataset or from IRIS
    * secondly, each batch will be split along the field specified by the
      AVAILABLE_TERRITORIAL_SPLITS_FOR_BORDERS constant, so the number of files
      should be greater by far.


    Parameters:
    -----------
    list_format : list
        A list of desired formats. For ex. ['geojson', 'topojson']
    years : list
        A list of desired vintages. For ex. [2023, 2024]
    crs_list : list
        A list of desired CRS (Coordinate Reference Systems).
        For ex. [4326, 2154]
    simplifications : list, optional
        A list of simplification for cross-product generation. The default is
        PIPELINE_SIMPLIFICATION_LEVELS.

    Returns:
    --------
    combinations : list
        A list of dicts used for commanding the generation of a downstream
        dataset.

        Each dict has 2 elements inside it, referenced by keys 'key' and 'config':
        [ {"key": ..., "config": ...},  ]

        1st element is a tuple of the following structure:
        (border, filter_by, year).
        For instance ('ARRONDISSEMENT', 'DEPARTEMENT', '2021')

        2nd element is a list of dict, each corresponding to a generated
        dictionnary of the following structure:

        {
            'mesh_init':
                initial raw geodataset's mesh ('COMMUNE', 'CANTON' or 'IRIS'),
            'geodata_provider':
                provider for raw geodataset,
            'geodata_dataset_family':
                dataset_family for raw dataset,
            'geodata_source':
                source for raw dataset,
            'dissolve_by':
                field to operate dissolution on (will result to 'borders'
                configuration of child dataset)
            'territory':
                field on which to split the dataset, generating a file for
                territory_#1, territory_#2, ... (will result to 'filter_by'
                configuration of child dataset)
            'format':
                desired format for downstream geodataset ('topojson', ...)
            'year':
                desired vintage for downstream geodataset (2024, ...)
            'crs':
                desired projection for downstream geodataset (4326, ...)
            'simplification':
                desired level of simplification for downstream geodataset as
                integer based percentage (0, 40, ...)
            'GEODATA_SOURCE':
                string to be used in a new field to trace the geodataset's
                source
            }

    Example:
    --------
    Example usage:
        >>> formats = ['geojson', 'gpkg']
        >>> years = [2022, 2023]
        >>> crs_list = [4326, 2154]
        >>> simplifications = [0, 40]
        >>> result = crossproduct_parameters_production(
               formats, years, crs_list, simplifications
            )
        >>> print(result)
        >>> {
            ('ARRONDISSEMENT', 'DEPARTEMENT', 2022): [
                {
                    'mesh_init': 'COMMUNE',
                    'geodata_provider': 'IGN',
                    'geodata_dataset_family': 'ADMINEXPRESS',
                    'geodata_source': 'EXPRESS-COG-CARTO-TERRITOIRE',
                    'dissolve_by': 'ARRONDISSEMENT',
                    'territory': 'DEPARTEMENT',
                    'format': 'geojson',
                    'year': 2022,
                    'crs': 4326,
                    'simplification': 0,
                    'GEODATA_SOURCE': "Cartiflette d'après IGN (EXPRESS-COG-CARTO-TERRITOIRE) simplifié à 0 %"
                },
                ...,
                {
                    'mesh_init': 'IRIS',
                    'geodata_provider': 'IGN',
                    'geodata_dataset_family': 'IRIS',
                    'geodata_source': 'CONTOUR-IRIS',
                    'dissolve_by': 'ARRONDISSEMENT',
                    'territory': 'DEPARTEMENT',
                    'format': 'gpkg',
                    'year': 2022,
                    'crs': 2154,
                    'simplification': 40,
                    'GEODATA_SOURCE': "Cartiflette d'après IGN (CONTOUR-IRIS) simplifié à 40 %"
                }
            ],
            ('ARRONDISSEMENT', 'DEPARTEMENT', 2023): [
                {
                    'mesh_init': 'COMMUNE',
                    'geodata_provider': 'IGN',
                    'geodata_dataset_family': 'ADMINEXPRESS',
                    'geodata_source': 'EXPRESS-COG-CARTO-TERRITOIRE',
                    'dissolve_by': 'ARRONDISSEMENT',
                    'territory': 'DEPARTEMENT',
                    'format': 'geojson',
                    'year': 2023,
                    'crs': 4326,
                    'simplification': 0,
                    'GEODATA_SOURCE': "Cartiflette d'après IGN (EXPRESS-COG-CARTO-TERRITOIRE) simplifié à 0 %"
                },
                ...,
                {
                    'mesh_init': 'IRIS',
                    'geodata_provider': 'IGN',
                    'geodata_dataset_family': 'IRIS',
                    'geodata_source': 'CONTOUR-IRIS',
                    'dissolve_by': 'ARRONDISSEMENT',
                    'territory': 'DEPARTEMENT',
                    'format': 'gpkg',
                    'year': 2023,
                    'crs': 2154,
                    'simplification': 40,
                    'GEODATA_SOURCE': "Cartiflette d'après IGN (CONTOUR-IRIS) simplifié à 40 %"
                }
            ],
            ...
        }
    """

    # prepare a list of (potential) sources from cartiflette's config
    # (the result will depend of the resolution in the config)
    sources = {
        "COMMUNE": PIPELINE_DOWNLOAD_ARGS["ADMIN-EXPRESS"],
        "IRIS": PIPELINE_DOWNLOAD_ARGS["IRIS"],
    }
    sources = pd.DataFrame(sources).T
    sources.columns = [
        "geodata_provider",
        "geodata_dataset_family",
        "geodata_source",
        "geodata_territorial_components",
    ]
    sources.index.name = "mesh_init"
    sources = sources.reset_index(drop=False)
    sources["geodata_territorial_components"] = (
        sources.geodata_territorial_components.apply(", ".join)
    )

    # prepare a list of tuples (
    #       administrative_level = polygon level = borders,
    #       territory used for splitting the file's boundaries = territory
    # ),
    croisement_filter_by_borders_flat = pd.DataFrame(
        flatten_dict_to_list(AVAILABLE_TERRITORIAL_SPLITS_FOR_BORDERS),
        columns=["borders", "territory"],
    )

    # prepare a list of tuples (
    #       raw source's polygon level,
    #       mesh created after dissolve
    # ),
    geometries_dissolutions = pd.DataFrame(
        flatten_dict_to_list(AVAILABLE_DISSOLUTIONS_FROM_RAW_MESH),
        columns=["mesh_init", "dissolve_by"],
    )

    combinations = sources.merge(
        geometries_dissolutions.merge(
            croisement_filter_by_borders_flat,
            left_on="dissolve_by",
            right_on="borders",
        )
    )
    combinations = (
        combinations.join(pd.Series(list_format, name="format"), how="cross")
        .join(pd.Series(years, name="year"), how="cross")
        .join(pd.Series(crs_list, name="crs"), how="cross")
        .join(pd.Series(simplifications, name="simplification"), how="cross")
    )
    combinations["GEODATA_SOURCE"] = (
        "Cartiflette d'après "
        + combinations["geodata_provider"]
        + " ("
        + combinations["geodata_source"]
        + ") simplifié à "
        + combinations["simplification"].astype(str)
        + " %"
    )
    combinations = combinations.drop(
        ["geodata_territorial_components", "borders"], axis=1
    )

    logger.debug(
        f"found {len(combinations)} combinations of downstream geodatasets"
    )

    combinations = (
        combinations.set_index(["dissolve_by", "territory", "year"])
        .groupby(["dissolve_by", "territory", "year"])
        .apply(lambda x: x.to_dict(orient="records"))
        .to_dict()
    )

    logger.info(f"will {len(combinations)} pods")

    return [{"key": key, "config": val} for key, val in combinations.items()]
