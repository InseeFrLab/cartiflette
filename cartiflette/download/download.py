# -*- coding: utf-8 -*-

# =============================================================================
# Inner functions to perform downloads and storage into s3. To run a full
# pipeline, please refer yourself to cartiflette\download\pipeline.py
# =============================================================================

from collections import OrderedDict
from itertools import product
import logging
from pebble import ThreadPool
import s3fs
import shutil
import traceback
from typing import Union

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS, THREADS_DOWNLOAD
from cartiflette.utils import (
    deep_dict_update,
    create_path_bucket,
)
from cartiflette.download.scraper import MasterScraper
from cartiflette.download.dataset import Dataset

logger = logging.getLogger(__name__)


def _upload_raw_dataset_to_s3(
    dataset: Dataset,
    result: dict,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> dict:
    """
    Upload a dataset's layers' objects into s3. In case of success, will also
    update the JSON md5 at the root of the filesystem and return a dict maping
    layers to the uploaded files (only the main file if this is a shapefile
    layer). Will perform a cleanup of the temporary folder whatever the result.

    Parameters
    ----------
    dataset : Dataset
        Dataset object to store into s3
    result : dict
        result of the dataset's download
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.

    Returns
    -------
    dict
        If upload fails, the dict will be empty. In any other case, it should
        map layers to each file (or the main file if this is a shapefile
        layer)

        Ex: {
            'CHEF_LIEU': [
                'projet-cartiflette/diffusion/shapefiles-test4/year=2017/administrative_level=None/crs=4326/None=None/vectorfile_format=shp/provider=IGN/dataset_family=BDTOPO/source=ROOT/territory=martinique/CHEF_LIEU.shp'
            ],
            'COMMUNE': [
                'projet-cartiflette/diffusion/shapefiles-test4/year=2017/administrative_level=None/crs=4326/None=None/vectorfile_format=shp/provider=IGN/dataset_family=BDTOPO/source=ROOT/territory=martinique/COMMUNE.shp'
            ],
            'ARRONDISSEMENT': [
                'projet-cartiflette/diffusion/shapefiles-test4/year=2017/administrative_level=None/crs=4326/None=None/vectorfile_format=shp/provider=IGN/dataset_family=BDTOPO/source=ROOT/territory=metropole/ARRONDISSEMENT.shp'
            ]
        }


    """

    if not result["downloaded"]:
        logger.info("File already there and uptodate")
        return

    try:
        # DUPLICATE SOURCES IN BUCKET
        errors_encountered = False
        dataset_paths = dict()
        for key, layer in result["layers"].items():
            layer_paths = []
            for path, rename_basename in layer.files_to_upload.items():
                path_within = create_path_bucket(
                    {
                        "bucket": bucket,
                        "path_within_bucket": path_within_bucket,
                        "year": layer.year,
                        "borders": None,
                        "crs": layer.crs,
                        "filter_by": "origin",
                        "value": "raw",
                        "vectorfile_format": layer.format,
                        "provider": layer.provider,
                        "dataset_family": layer.dataset_family,
                        "source": layer.source,
                        "territory": layer.territory,
                        "simplification": None,
                        "filename": rename_basename,
                    }
                )

                layer_paths.append(path_within)

                logger.debug(f"upload to {path_within}")

                try:
                    fs.put(path, path_within, recursive=True)
                except Exception as e:
                    logger.error(e)
                    errors_encountered = True

            if any(x.lower().endswith(".shp") for x in layer_paths):
                layer_paths = [x for x in layer_paths if x.lower().endswith(".shp")]

            dataset_paths[key] = layer_paths

    except Exception as e:
        logger.error(e)
        errors_encountered = True

    finally:
        # cleanup temp files
        shutil.rmtree(result["root_cleanup"])

    if not errors_encountered:
        # NOW WRITE MD5 IN BUCKET ROOT (in case of error, should be skipped
        # to allow for further tentatives)
        dataset.update_json_md5(result["hash"])
        return dataset_paths
    else:
        return {}


def _download_sources(
    providers: Union[list[str, ...], str],
    dataset_families: Union[list[str, ...], str],
    sources: Union[list[str, ...], str],
    territories: Union[list[str, ...], str],
    years: Union[list[str, ...], str],
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
    upload: bool = True,
) -> dict:
    # TODO : contrôler return
    """
    Main function to perform downloads of datasets and store them the s3.
    All available combinations will be tested; hence an unfound file might not
    be an error given the fact that it might correspond to an unexpected
    combination; those will be printed as warnings in the log.


    Parameters
    ----------
    providers : list[str, ...]
        List of providers in the yaml file
    dataset_families : list[str, ...]
        List of datasets family in the yaml file
    sources : list[str, ...]
        List of sources in the yaml file
    territories : list[str, ...]
        List of territoires in the yaml file
    years : list[int, ...]
        List of years in the yaml file
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.
    upload : bool, optional
        Use for debugging: whether to store the files into the s3 or not.
        The default is True.

    Returns
    -------
    dict
        DESCRIPTION.


    files : dict
        Structure of the nested dict will use the following keys :
            provider
                dataset_family
                    source
                        territory
                            year
                                {downloaded: bool, paths: list:str}
        For instance:
            {
                'IGN': {
                    'BDTOPO': {
                        'ROOT': {
                            'france_entiere': {
                                2017: {
                                    'downloaded': True,
                                    'paths': {
                                        'CHEF_LIEU': [
                                            'projet-cartiflette/diffusion/shapefiles-test4/year=2017/administrative_level=None/crs=4326/None=None/vectorfile_format=shp/provider=IGN/dataset_family=BDTOPO/source=ROOT/territory=martinique/CHEF_LIEU.shp'
                                        ],
                                        'COMMUNE': [
                                            'projet-cartiflette/diffusion/shapefiles-test4/year=2017/administrative_level=None/crs=4326/None=None/vectorfile_format=shp/provider=IGN/dataset_family=BDTOPO/source=ROOT/territory=martinique/COMMUNE.shp'
                                        ],
                                        'ARRONDISSEMENT': [
                                            'projet-cartiflette/diffusion/shapefiles-test4/year=2017/administrative_level=None/crs=4326/None=None/vectorfile_format=shp/provider=IGN/dataset_family=BDTOPO/source=ROOT/territory=metropole/ARRONDISSEMENT.shp'
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
    """
    kwargs = OrderedDict()
    items = [
        ("sources", sources),
        ("territories", territories),
        ("years", years),
        ("providers", providers),
        ("dataset_families", dataset_families),
    ]
    for key, val in items:
        if isinstance(val, str) or isinstance(val, int):
            kwargs[key] = [val]
        elif not val:
            kwargs[key] = [None]
        elif isinstance(val, list) or isinstance(val, tuple) or isinstance(val, set):
            kwargs[key] = list(val)

    combinations = list(product(*kwargs.values()))

    files = {}
    with MasterScraper() as s:

        def func(args):
            source, territory, year, provider, dataset_family = args
            datafile = Dataset(
                dataset_family,
                source,
                year,
                provider,
                territory,
                bucket,
                path_within_bucket,
            )
            try:
                result = s.download_unpack(datafile)
            except ValueError as e:
                logger.warning(e)

                this_result = {
                    provider: {
                        dataset_family: {
                            source: {
                                territory: {
                                    year: {
                                        "downloaded": False,
                                        "paths": None,
                                    }
                                }
                            }
                        }
                    }
                }

            else:
                if upload:
                    paths = _upload_raw_dataset_to_s3(
                        datafile, result, bucket, path_within_bucket, fs
                    )
                else:
                    paths = {}
                    # cleanup temp files
                    if result["root_cleanup"]:
                        shutil.rmtree(result["root_cleanup"])

                del result["hash"], result["root_cleanup"], result["layers"]
                result["paths"] = paths

                this_result = {
                    provider: {dataset_family: {source: {territory: {year: result}}}}
                }

            return this_result

        if THREADS_DOWNLOAD > 1:
            with ThreadPool(THREADS_DOWNLOAD) as pool:
                iterator = pool.map(func, combinations).result()
                while True:
                    try:
                        files = deep_dict_update(files, next(iterator))
                    except StopIteration:
                        break
                    except Exception as e:
                        logger.error(e)
                        logger.error(traceback.format_exc())
        else:
            for args in combinations:
                files = deep_dict_update(files, func(args))

    return files
