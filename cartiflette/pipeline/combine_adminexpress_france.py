#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import contextlib
from copy import deepcopy
import logging
import os
import re
from tempfile import TemporaryDirectory
from typing import Union, List
import warnings

import s3fs

from cartiflette.config import (
    FS,
    BUCKET,
    PATH_WITHIN_BUCKET,
    PIPELINE_SIMPLIFICATION_LEVELS,
)
from cartiflette.s3.geodataset import (
    S3GeoDataset,
    concat_s3geodataset,
)


COMPILED_TERRITORY = re.compile("territory=([a-z]*)/", flags=re.IGNORECASE)


def combine_adminexpress_territory(
    year: Union[str, int],
    intermediate_dir: str = "temp",
    format_output: str = "geojson",
    simplifications_values: List[int] = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
    simplifications: List[int] = None,
) -> str:
    """
    Merge cities datasets into a single file (full France territory).

    All files are retrieved from S3, projected to mercator coordinates, then
    merged using mapshaper. Every computation is done on the disk, inside
    a temporary file.

    Parameters
    ----------
    year : Union[str, int]
        Desired vintage
    intermediate_dir : str, optional
        Temporary dir to process files. The default is "temp".
    format_output : str, optional
        Final (and intermediate) formats to use. The default is "topojson"
    simplifications_values : List[int], optional
        List of simplifications' levels to compute (as percentage values
        casted to integers). The default is None, which will result to
        PIPELINE_SIMPLIFICATION_LEVELS.
    bucket : str, optional
        Storage bucket on S3 FileSystem. The default is BUCKET.
    path_within_bucket : str, optional
        Path within S3 bucket used for storage. The default is
        PATH_WITHIN_BUCKET.
    fs : s3fs.FyleSystem, optional
        S3 file system used for storage of raw data. The default is FS.

    Returns
    -------
    output_path : str
        Path of uploaded dataset on S3.

    """
    if not simplifications_values:
        simplifications_values = PIPELINE_SIMPLIFICATION_LEVELS

    path = (
        f"{bucket}/{path_within_bucket}/"
        "provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-CARTO-TERRITOIRE/"
        f"year={year}/"
        "administrative_level=None/"
        "crs=*/"
        "origin=raw/"
        "vectorfile_format=*/"
        "territory=*/"
        "simplification=*/"
        "COMMUNE.*"
    )

    communes_paths = fs.glob(path)
    dirs = {os.path.dirname(x) for x in communes_paths}
    territories = {t for x in dirs for t in COMPILED_TERRITORY.findall(x)}

    if not territories:
        warnings.warn(f"{year} not constructed (no territories)")
        return

    logging.info("Territoires identifiés:\n%s", "\n".join(territories))

    config = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "provider": "IGN",
        "dataset_family": "ADMINEXPRESS",
        "source": "EXPRESS-COG-CARTO-TERRITOIRE",
        "borders": None,
        "crs": "*",
        "filter_by": "origin",
        "value": "raw",
        "vectorfile_format": "shp",
        "simplification": 0,
        "year": year,
        "filename": "COMMUNE",
    }

    uploaded = []

    for mesh in "COMMUNE", "CANTON":
        mesh_config = deepcopy(config)
        mesh_config["filename"] = mesh

        datasets = [{"territory": territory} for territory in territories]
        for d in datasets:
            d.update(mesh_config)

        mesh_config.update(
            {
                "vectorfile_format": format_output,
                "crs": 4326,
                "borders": mesh,
                "filter_by": "preprocessed",
                "value": "before_cog",
                "territory": "france",
                "provider": "Cartiflette",
                "dataset_family": "geodata",
            }
        )

        with TemporaryDirectory() as tempdir:
            input_geodatasets = [
                S3GeoDataset(fs=fs, **config) for config in datasets
            ]
            with contextlib.ExitStack() as stack:
                # download all datasets in context: download at enter, clean
                # disk at exit
                input_geodatasets = [
                    stack.enter_context(dset) for dset in input_geodatasets
                ]

                dset = concat_s3geodataset(
                    input_geodatasets,
                    fs=fs,
                    output_dir=tempdir,
                    **mesh_config,
                )

            for simplification in simplifications_values:
                with dset.copy() as new_dset:
                    logging.info("-+" * 25)
                    logging.info(
                        "Create %s geodatasets with simplification=%s",
                        mesh,
                        simplification,
                    )
                    logging.info("-+" * 25)

                    # Simplify the dataset
                    new_dset.simplify(
                        format_output=format_output,
                        simplification=simplification,
                    )
                    new_dset.to_s3()

                    uploaded.append(new_dset.s3_dirpath)

                if mesh == "COMMUNE":
                    with dset.copy() as new_dset:
                        # also make derived geodatasets based on municipal
                        # districts mesh
                        logging.info("-" * 50)
                        logging.info(
                            "Also computing geodatasets with communal "
                            "districts"
                        )
                        logging.info("-" * 50)

                        with new_dset.substitute_municipal_districts(
                            format_output=format_output
                        ) as communal_districts:
                            communal_districts.simplify(
                                format_output=format_output,
                                simplification=simplification,
                            )
                            communal_districts.to_s3()
                            uploaded.append(communal_districts.s3_dirpath)

    return uploaded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    combine_adminexpress_territory(2023, format_output="geojson")
