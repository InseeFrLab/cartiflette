#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import ExitStack, nullcontext
from copy import deepcopy
from functools import partial
from itertools import product
import logging
import os
import re
from tempfile import TemporaryDirectory
import traceback
from typing import Union, List
import warnings

from pebble import ThreadPool
import s3fs

from cartiflette.config import (
    FS,
    BUCKET,
    PATH_WITHIN_BUCKET,
    PIPELINE_SIMPLIFICATION_LEVELS,
    THREADS_DOWNLOAD,
    PIPELINE_DOWNLOAD_ARGS,
)
from cartiflette.s3.geodataset import (
    S3GeoDataset,
    concat_s3geodataset,
)

logger = logging.getLogger(__name__)


COMPILED_TERRITORY = re.compile(r"territory=([a-z\-]*)/", flags=re.IGNORECASE)


def make_one_geodataset(
    dset: S3GeoDataset,
    with_municipal_district: bool,
    simplification: int,
    communal_districts: S3GeoDataset = None,
    format_output: str = "geojson",
) -> str:
    """
    Generate one geodataset and upload it to S3FileSystem

    Parameters
    ----------
    dset : S3GeoDataset
        Basic geodataset with full France coverage, already downloaded. This
        dataset should have a basic geometric mesh coherent with the `mesh`
        argument. At the time of the docstring redaction, the dataset should
        either be composed of cities or cantons.
    with_municipal_district : bool
        Whether to substitutes main cities (Paris, Lyon, Marseille) with
        their municipal districts. Obviously, this can only be used with a
        cities dataset.
    simplification : int
        Level of desired simplification.
    communal_districts : S3GeoDataset, optional
        Geodataset for communal districts, already downloaded. Only needed if
        `mesh == 'COMMUNE'`. The default is None.
    format_output : str, optional
        Final format to use. The default is "geojson".

    Returns
    -------
    uploaded : str
        Uploaded file's path on S3FileSystem

    """

    mesh = dset.config["borders"]

    if mesh != "COMMUNE" and with_municipal_district:
        raise ValueError(
            "with_municipal_district is not authorized with this S3GeoDataset "
            f"(found {mesh=} instead of 'COMMUNE')"
        )

    log = "Create %s geodatasets with simplification=%s"
    if with_municipal_district:
        log += " with municipal districts substitution"
    logger.info(log, mesh, simplification)

    kwargs = {"format_output": format_output}

    new_dset = dset.copy()
    if with_municipal_district:
        # substitute communal districts
        districts = new_dset.substitute_municipal_districts(
            communal_districts=communal_districts.copy(), **kwargs
        )
    else:
        districts = nullcontext()
    with new_dset, districts:
        processed_dset = districts if with_municipal_district else new_dset
        processed_dset.simplify(simplification=simplification, **kwargs)
        processed_dset.to_s3()
        uploaded = processed_dset.s3_dirpath

    return uploaded


def create_one_year_geodataset_batch(
    year: Union[str, int],
    format_output: str = "geojson",
    simplifications_values: List[int] = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> List[str]:
    """
    Merge cities datasets into a single file (full France territory).

    All files are retrieved from S3, projected to mercator coordinates, then
    merged using mapshaper. Every computation is done on the disk, inside
    a temporary dir.

    Note that multithreading will be used .
    To debug code, please consider deactivating the threading, using
    `cartiflette.config.THREADS = 1` beforehand.

    Parameters
    ----------
    year : Union[str, int]
        Desired vintage
    format_output : str, optional
        Final (and intermediate) formats to use. The default is "geojson"
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
    uploaded : List[str]
        Paths on S3 of uploaded datasets.

    """
    if not simplifications_values:
        simplifications_values = PIPELINE_SIMPLIFICATION_LEVELS

    paths = (
        f"{bucket}/{path_within_bucket}/"
        "provider=IGN/dataset_family=*/"
        "source=*/"
        f"year={year}/"
        "administrative_level=None/"
        "crs=*/"
        "origin=raw/"
        "vectorfile_format=*/"
        "territory=*/**/*.shp"
    )

    paths = fs.glob(paths)
    dirs = {os.path.dirname(x) for x in paths}
    territories = {t for x in dirs for t in COMPILED_TERRITORY.findall(x)}
    territories = territories - {"france_entiere"}

    if not territories:
        warnings.warn(f"{year} not constructed (no territories available)")
        return

    logger.info("Territoires identifiés:\n%s", "\n".join(territories))

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
        "fs": fs,
    }

    uploaded = []

    # Construct S3GeoDataset for municipal districts
    raw_config = deepcopy(config)
    kwargs = {"territory": "metropole", "filename": "ARRONDISSEMENT_MUNICIPAL"}
    districts = S3GeoDataset(**kwargs, **raw_config)

    input_geodatasets = {}
    # Retrieve raw files of cities, cantons and iris
    dset_source_configs = {
        "COMMUNE": PIPELINE_DOWNLOAD_ARGS["ADMIN-EXPRESS"][:3],
        "CANTON": PIPELINE_DOWNLOAD_ARGS["ADMIN-EXPRESS"][:3],
        "IRIS": PIPELINE_DOWNLOAD_ARGS["IRIS"][:3],
    }
    for mesh in "CANTON", "COMMUNE", "IRIS":

        provider, family, source = dset_source_configs[mesh]
        # Construct S3GeoDatasets for each territory (Guyane, metropole, ...)
        # at mesh level (COMMUNE or CANTON)
        mesh_config = deepcopy(config)
        mesh_config["provider"] = provider
        # Nota : filename for IRIS might be CONTOURS-IRIS.shp or IRIS_GE.shp
        # while COMMUNE and CANTON are COMMUNE.shp and CANTON.shp
        mesh_config["filename"] = f"*{mesh}*"
        mesh_config["dataset_family"] = family
        mesh_config["source"] = source
        geodatasets = []
        for territory in territories:
            try:
                geodatasets.append(
                    S3GeoDataset(territory=territory, **mesh_config)
                )
            except ValueError:
                # not present for this territory and this mesh
                logger.warning(
                    "file not found for %s on mesh=%s", territory, mesh
                )
                continue

        with TemporaryDirectory() as tempdir:
            with ExitStack() as stack:
                # download all datasets in context: download at enter
                if THREADS_DOWNLOAD > 1:
                    threads = min(THREADS_DOWNLOAD, len(geodatasets))
                    with ThreadPool(threads) as pool:
                        geodatasets = list(
                            pool.map(
                                stack.enter_context,
                                geodatasets,
                                timeout=60 * 2,
                            ).result()
                        )
                else:
                    geodatasets = [
                        stack.enter_context(dset) for dset in geodatasets
                    ]

                if not geodatasets:
                    logger.warning(
                        "base geodataset from mesh=%s was not generated", mesh
                    )
                    continue

                # concat S3GeoDataset
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
                dset = concat_s3geodataset(
                    geodatasets,
                    output_dir=tempdir,
                    output_name=mesh,
                    **mesh_config,
                )

                input_geodatasets[mesh] = dset.copy()

                # clean intermediate datasets from local disk at exit (keep
                # only concatenated S3GeoDataset, which exists only on local
                # disk)

    with input_geodatasets["COMMUNE"] as commune, input_geodatasets[
        "CANTON"
    ] as canton, input_geodatasets["IRIS"] as iris, districts as districts:
        # download communal_districts and enter context for commune/canton/iris

        args = (
            list(product([commune], [True, False], simplifications_values))
            + list((product([canton], [False], simplifications_values)))
            + list((product([iris], [False], simplifications_values)))
        )

        func = partial(
            make_one_geodataset,
            format_output=format_output,
            communal_districts=districts,
        )

        if THREADS_DOWNLOAD > 1:
            # create geodatasets with multithreading
            threads = min(THREADS_DOWNLOAD, len(args))
            logger.info(
                "Parallelizing simplifications with %s threads", threads
            )
            with ThreadPool(threads) as pool:
                iterator = pool.map(func, *list(zip(*args))).result()

                while True:
                    try:
                        uploaded.append(next(iterator))
                    except StopIteration:
                        break
                    except Exception:
                        logger.error(traceback.format_exc())
        else:
            # create geodatasets using a simple loop
            for dset, with_municipal_district, simplification in args:
                try:
                    uploaded.append(
                        func(
                            dset=dset,
                            with_municipal_district=with_municipal_district,
                            simplification=simplification,
                        )
                    )
                except Exception:
                    logger.error(traceback.format_exc())

    return uploaded


def make_all_geodatasets(
    years: List[int],
    format_output: str = "geojson",
    simplifications_values: List[int] = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> dict:
    """
    Create all base geodatasets used by cartiflette (full France coverage).
    At the time of writing, the meshes can either be COMMUNE or CANTON.

    Note that multithreading will be used under the hood for each yearly batch.
    To debug code, please consider deactivating the threading, using
    `cartiflette.config.THREADS = 1` beforehand.

    Parameters
    ----------
    years : List[int]
        Desired vintages.
    format_output : str, optional
        Final (and intermediate) formats to use. The default is "geojson"
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
    uploaded : dict
        Dict of uploaded geodatasets on S3 using the following structure:
        {year : [path_file1, path_file2, ...]}.

    """

    if not simplifications_values:
        simplifications_values = PIPELINE_SIMPLIFICATION_LEVELS

    uploaded = []

    for year in years:
        logger.info("-" * 50)
        logger.info(f"Merging territorial files of cities for {year=}")
        logger.info("-" * 50)

        try:
            # Merge all territorial cities files into a single file
            uploaded_year = create_one_year_geodataset_batch(
                year=year,
                path_within_bucket=path_within_bucket,
                format_output=format_output,
                bucket=bucket,
                fs=fs,
                simplifications_values=simplifications_values,
            )

            if not uploaded_year:
                # No files merged
                continue

            uploaded.append({year: uploaded_year})

        except Exception as e:
            warnings.warn(f"geodataset {year=} not created: {e}")

    return uploaded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    created = create_one_year_geodataset_batch(2023, format_output="geojson")
