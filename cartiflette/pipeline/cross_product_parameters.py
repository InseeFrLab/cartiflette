from collections import OrderedDict
import logging

import pandas as pd
from pebble import ThreadPool
from s3fs import S3FileSystem

from cartiflette.config import (
    FS,
    BUCKET,
    PATH_WITHIN_BUCKET,
    THREADS_DOWNLOAD,
    INTERMEDIATE_FORMAT,
)
from cartiflette.pipeline_constants import (
    AVAILABLE_DISSOLUTIONS_FROM_RAW_MESH,
    AVAILABLE_TERRITORIAL_SPLITS_FOR_BORDERS,
    PIPELINE_DOWNLOAD_ARGS,
    PIPELINE_SIMPLIFICATION_LEVELS,
)
from cartiflette.s3 import S3GeoDataset

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
            A list of lists containing key-value pairs obtained by flattening
            the input dictionary.
    ValueError
        Example:
        --------
        Example usage:
            sample_dict = {'a': [1, 2, 3], 'b': [4, 5]}
            result = flatten_dict(sample_dict)
            print(result)

        This will output:
            [['a', 1], ['a', 2], ['a', 3], ['b', ValueError4], ['b', 5]]
    """
    flattened_list = [
        [key, inner_value]
        for key, values in dict_with_list.items()
        for inner_value in values
    ]

    return flattened_list


def multiindex_to_nested_dict(
    df: pd.DataFrame, value_only=False
) -> OrderedDict:
    if isinstance(df.index, pd.MultiIndex):
        return OrderedDict(
            (k, multiindex_to_nested_dict(df.loc[k]))
            for k in df.index.remove_unused_levels().levels[0]
        )
    else:
        if value_only:
            return OrderedDict((k, df.loc[k].values[0]) for k in df.index)
        else:
            d = OrderedDict()
            for idx in df.index:
                d_col = OrderedDict()
                for col in df.columns:
                    d_col[col] = df.loc[idx, col]
                d[idx] = d_col
            return d


def crossproduct_parameters_production(
    list_format: list,
    year: int,
    crs_list: list,
    simplifications: list = PIPELINE_SIMPLIFICATION_LEVELS,
    fs: S3FileSystem = FS,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
) -> list:
    """
    Generates a dict of arguments commanding the generation of output
    geodatasets. Only the best available process to generate a given dataset
    are kep (for instance among available IRIS and COMMUNE candidates).

    Note that the length of the return represents the number of downstream pods.

    Parameters:
    -----------
    list_format : list
        A list of desired formats. For ex. ['geojson', 'topojson']
    year : int
        Desired vintage. For ex. 2023
    crs_list : list
        A list of desired CRS (Coordinate Reference Systems).
        For ex. [4326, 2154]
    simplifications : list, optional
        A list of simplification for cross-product generation. The default is
        PIPELINE_SIMPLIFICATION_LEVELS.
    fs : S3FileSystem, optional
        S3FileSystem used for storage. The default is FS.
    bucket : str, optional
        The bucket used for storage on fs. The default is BUCKET.
    path_within_bucket : str, optional
        The path within the bucket used for storage on fs. The default is
        PATH_WITHIN_BUCKET.

    Returns:
    --------
    combinations : list
        A list of dicts used for commanding the generation of a downstream
        dataset.

        Each dict has 5 keys:
            * mesh_init: str (for instance 'DEPARTEMENT')
            * source_geodata: str (for instance 'EXPRESS-COG-CARTO-TERRITOIRE')
            * simplification: str (for instance '2021')
            * dissolve_by: str (for instance 'ARRONDISSEMENT')
            * config: List[dict]

        Each config dictionnary has the following structure and should
        correspond to a specific geodataset to be generated.

        {
            crs (projection, for inst. 4326) : [
                {
                    'territory':
                        territorial split ('REGION', 'FRANCE_ENTIERE', ...)
                    'format':
                        desired format ('topojson', ...)
                }, ...
            ]
        }

    Example:
    --------
    Example usage:
        >>> formats = ['geojson', 'gpkg']
        >>> year = 2023
        >>> crs_list = [4326, 2154]
        >>> simplifications = [0, 40]
        >>> result = crossproduct_parameters_production(
               formats, year, crs_list, simplifications
            )
        >>> print(result)
        >>> [
            {
                'mesh_init': 'CANTON',
                'source_geodata': 'EXPRESS-COG-CARTO-TERRITOIRE',
                'simplification': 0,
                'dissolve_by': 'CANTON',
                'config': {
                    2154: [{
                            'territory': 'TERRITOIRE',
                            'format': 'gpkg'
                        },
                        ...,
                        {
                            'territory': 'DEPARTEMENT',
                            'format': 'geojson'
                        }
                    ],
                    4326: [{
                            'territory': 'TERRITOIRE',
                            'format': 'gpkg'
                        },
                        ...,
                    ]
                }
            }, {
                'mesh_init': 'CANTON',
                'source_geodata': 'EXPRESS-COG-CARTO-TERRITOIRE',
                'simplification': 40,
                'dissolve_by': 'CANTON',
                'config': {...}
            }
        ]
    """

    # prepare a list of (potential) sources from cartiflette's config
    # (the result will depend of the resolution in the config)
    sources = {
        "COMMUNE": PIPELINE_DOWNLOAD_ARGS["ADMIN-EXPRESS"],
        "IRIS": PIPELINE_DOWNLOAD_ARGS["IRIS"],
        "CANTON": PIPELINE_DOWNLOAD_ARGS["ADMIN-EXPRESS"],
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

    sources = sources.drop(
        ["geodata_provider", "geodata_dataset_family"], axis=1
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
        .join(pd.Series(crs_list, name="crs"), how="cross")
        .join(pd.Series(simplifications, name="simplification"), how="cross")
    )

    combinations = combinations.drop(
        ["geodata_territorial_components", "borders"], axis=1
    )

    def geodataset_exists(borders, geodata_source, simplification):
        "check if preprocessed geodata file is found on S3"
        config = {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "provider": "Cartiflette",
            "dataset_family": "geodata",
            "source": geodata_source,
            "year": year,
            "borders": borders,
            "crs": 4326,
            "filter_by": "preprocessed",
            "value": "before_cog",
            "vectorfile_format": INTERMEDIATE_FORMAT,
            "territory": "france",
            "simplification": simplification,
            "fs": fs,
        }
        try:
            S3GeoDataset(**config, build_from_local=False)
            return True
        except ValueError:
            # raw file does not exist
            return False

    def metadataset_exists(borders):
        "check if preprocessed metadata file is found on S3"
        config = {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "provider": "Cartiflette",
            "dataset_family": "metadata",
            "source": "*",
            "year": year,
            "borders": borders,
            "crs": None,
            "filter_by": "preprocessed",
            "value": "tagc",
            "vectorfile_format": "csv",
            "territory": "france",
            "simplification": 0,
            "fs": fs,
        }
        try:
            S3GeoDataset(**config, build_from_local=False)
            return True
        except ValueError:
            # raw file does not exist
            return False

    # remove combinations having no available upstream source
    geodata_unique = combinations[
        ["mesh_init", "geodata_source", "simplification"]
    ].drop_duplicates()
    metadata_unique = combinations[["mesh_init"]].drop_duplicates()

    if THREADS_DOWNLOAD == 1:

        geodata_unique["upstream_geodata_exists"] = geodata_unique.apply(
            lambda tup: geodataset_exists(*tup), axis=1
        )

        metadata_unique["upstream_metadata_exists"] = metadata_unique.apply(
            lambda tup: metadataset_exists(*tup), axis=1
        )

    else:
        with ThreadPool(min(THREADS_DOWNLOAD, len(combinations))) as pool:
            geodata_unique["upstream_geodata_exists"] = list(
                pool.map(
                    geodataset_exists, *zip(*geodata_unique.values.tolist())
                ).result()
            )

            metadata_unique["upstream_metadata_exists"] = list(
                pool.map(
                    metadataset_exists, *zip(*metadata_unique.values.tolist())
                ).result()
            )

    combinations = combinations.merge(geodata_unique).merge(metadata_unique)
    combinations["upstream_exists"] = (
        combinations["upstream_geodata_exists"]
        & combinations["upstream_metadata_exists"]
    )

    ix = combinations[~combinations.upstream_exists].index
    combinations = combinations.drop(ix).drop(
        [
            "upstream_exists",
            "upstream_geodata_exists",
            "upstream_metadata_exists",
        ],
        axis=1,
    )

    logger.debug(
        f"found {len(combinations)} combinations of downstream geodatasets"
    )

    # get best combination available among COMMUNE/IRIS/CANTON
    # -> for each geodataset to generate, keep COMMUNE if available, IRIS
    # otherwise (and CANTON for border=CANTON generation)
    dups = [
        "dissolve_by",
        "territory",
        "format",
        "crs",
        "simplification",
        "mesh_init",
    ]
    combinations = combinations.sort_values(dups, ascending=False)
    combinations = combinations.drop_duplicates(dups[:-1], keep="last")

    def cascade_dict(df, keys: list):
        try:
            return (
                df.set_index(keys[0])
                .groupby(keys[0])
                .apply(lambda x: cascade_dict(x, keys[1:]))
            ).to_dict()
        except (AttributeError, IndexError, ValueError):
            return (
                df.set_index(keys)
                .groupby(keys)
                .apply(lambda x: x.to_dict(orient="records"))
                .to_dict()
            )

    combinations = cascade_dict(
        combinations,
        [
            ["mesh_init", "geodata_source", "simplification", "dissolve_by"],
            "crs",
        ],
    )
    logger.info(f"{len(combinations)} pods will be created")

    combinations = [
        {
            "mesh_init": key[0],
            "source_geodata": key[1],
            "simplification": key[2],
            "dissolve_by": key[3],
            "config": val,
        }
        for key, val in combinations.items()
    ]

    return combinations
