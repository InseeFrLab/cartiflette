# -*- coding: utf-8 -*-
"""
Created on Sat Dec 14 19:18:05 2024
"""

import json
import logging
import re

import pandas as pd
from s3fs import S3FileSystem

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET

logger = logging.getLogger(__name__)


def nested_dict_from_multiindex(df: pd.DataFrame) -> dict:
    """
    Convenience function to transform a multiindexed DataFrame do a nested
    dict, minimizing the dict's size.

    Parameters
    ----------
    df : pd.DataFrame
        Multiindexed DataFrame.

    Returns
    -------
    dict
        Nested dictionnary

    """

    result = {}
    for idx, value in df["simplification"].items():
        d_ref = result
        for key in idx[:-1]:
            if key not in d_ref:
                d_ref[key] = {}
            d_ref = d_ref[key]
        d_ref[idx[-1]] = value
    return result


def flatten_dict(d: dict, parent_key: tuple = ()) -> dict:
    """
    Convenience function, flattens a nested dictionary and convert it back to
    dataframe.

    Parameters
    ----------
    d : dict
        Nested dictionary
    parent_key : tuple, optional
        Optional key, used for recursive purposes. The default is ().

    Returns
    -------
    dict
        flattened dictionnary

    """
    items = []
    for k, v in d.items():
        new_key = parent_key + (k,)
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)


def make_s3_inventory(
    fs: S3FileSystem = FS,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
):
    """
    Compute an inventory of all datasets generated by Cartiflette and push it
    to the S3 File System as a single json file.

    The json is pushed to f"{bucket}/{path_within_bucket}/inventory.json". It
    uses a nested dictionnary format to ensure the json is small enough to
    enhance download performances.

    Parameters
    ----------
    fs : S3FileSystem, optional
        S3 File System. The default is FS.
    bucket : str, optional
        Used bucket (both for inventory querying and json storage). The default
        is BUCKET.
    path_within_bucket : str, optional
        Path used within bucket. The default is PATH_WITHIN_BUCKET.

    Returns
    -------
    None.

    """

    paths = (
        f"{bucket}/{path_within_bucket}/"
        "provider=Cartiflette/dataset_family=production/"
        "**/*"
    )
    # debug
    # paths = (
    #     f"{bucket}/{path_within_bucket}/"
    #     "provider=Cartiflette/dataset_family=production/"
    #     "source=CONTOUR-IRIS/"
    #     "year=2023/"
    #     "administrative_level=IRIS/"
    #     "crs=4326/"
    #     "**/*"
    # )

    paths = fs.glob(paths)

    compiled = re.compile(
        ".*?/"
        "source=(?P<source>.*?)/"
        "year=(?P<year>[0-9]{4})/"
        "administrative_level=(?P<administrative_level>.*?)/"
        "crs=(?P<crs>[0-9]{4})/"
        "(?P<filter_by>.*?)=(?P<value>.*?)/"
        "vectorfile_format=(?P<vectorfile_format>.*?)/"
        "territory=(?P<territory>.*?)/"
        "simplification=(?P<simplification>[0-9]*?)/"
        ".*"
    )

    datasets = [
        x.groupdict() for path in paths for x in compiled.finditer(path)
    ]
    datasets = pd.DataFrame(datasets)

    cols = [
        "source",
        "year",
        "administrative_level",
        "crs",
        "filter_by",
        "value",
        "vectorfile_format",
        "territory",
    ]
    datasets = datasets.set_index(cols)

    with fs.open(
        f"{bucket}/{path_within_bucket}/inventory.json", "w", encoding="utf8"
    ) as f:
        d = nested_dict_from_multiindex(datasets)
        json.dump(d, f)


def parse_inventory(
    fs: S3FileSystem = FS,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
) -> pd.DataFrame:
    """
    Retrieve and load cartiflette's current datasets' inventory (as a
    dataframe).

    Inventory columns are [
         'source',
         'year',
         'administrative_level',
         'crs',
         'filter_by',
         'value',
         'vectorfile_format',
         'territory',
         'simplification'
         ]

    Each row corresponds to an available DataFrame.

    Parameters
    ----------
    fs : S3FileSystem, optional
        S3 File System. The default is FS.
    bucket : str, optional
        Used bucket (both for inventory querying and json storage). The default
        is BUCKET.
    path_within_bucket : str, optional
        Path used within bucket. The default is PATH_WITHIN_BUCKET.

    Returns
    -------
    df : pd.DataFrame
        Inventory DataFrame

    """
    with fs.open(
        f"{bucket}/{path_within_bucket}/inventory.json", "r", encoding="utf8"
    ) as f:
        d = json.load(f)

    d = flatten_dict(d)
    # Convert the flattened dictionary to a DataFrame
    index = pd.MultiIndex.from_tuples(d.keys())
    df = pd.DataFrame(
        list(d.values()), index=index, columns=["simplification"]
    )
    index.names = [
        "source",
        "year",
        "administrative_level",
        "crs",
        "filter_by",
        "value",
        "vectorfile_format",
        "territory",
    ]

    df = df.reset_index(drop=False)
    return df


if __name__ == "__main__":
    # df = make_s3_inventory()
    df = parse_inventory()
    print(df)
