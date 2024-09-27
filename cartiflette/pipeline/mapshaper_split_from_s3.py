#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import traceback
from typing import List

from pebble import ThreadPool
from s3fs import S3FileSystem


from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
    INTERMEDIATE_FORMAT,
    THREADS_DOWNLOAD,
)
from cartiflette.s3 import S3GeoDataset, S3Dataset


logger = logging.getLogger(__name__)


def mapshaperize_split_from_s3(
    year: int,
    init_geometry_level: str,
    source: str,
    simplification: int,
    dissolve_by: str,
    config_generation: dict,
    fs: S3FileSystem = FS,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
):
    logger.info(
        "processing %s from '%s' geometries and dissolve on '%s'",
        year,
        init_geometry_level,
        dissolve_by,
    )

    kwargs = {
        "fs": fs,
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "year": year,
        "borders": init_geometry_level,
        "filter_by": "preprocessed",
        "provider": "Cartiflette",
        "territory": "france",
    }
    with S3Dataset(
        dataset_family="metadata",
        source="*",
        crs=None,
        value="tagc",
        vectorfile_format="csv",
        **kwargs,
    ) as metadata, S3GeoDataset(
        dataset_family="geodata",
        source=source,
        crs=4326,
        value="before_cog",
        vectorfile_format=INTERMEDIATE_FORMAT,
        simplification=simplification,
        **kwargs,
    ) as gis_file:

        failed = []
        success = []
        skipped = []
        for niveau_agreg, territory_configs in config_generation.items():

            # Check that both niveau_agreg and dissolve_by correspond to
            # definitive fields from either metadata/geodata
            available = set(gis_file._get_columns()) | set(
                metadata._get_columns()
            )

            warnings = []
            for field in niveau_agreg, dissolve_by:
                if field in [
                    "FRANCE_ENTIERE",
                    "FRANCE_ENTIERE_DROM_RAPPROCHES",
                ]:
                    continue
                try:
                    metadata.find_column_name(field, available)
                except (ValueError, IndexError) as exc:
                    warnings.append(str(exc))
            if warnings:
                skipped.append(
                    {
                        "warning": " - ".join(warnings),
                        "aggreg": niveau_agreg,
                        "config": territory_configs,
                    }
                )
                continue

            with gis_file.copy() as gis_copy:
                try:
                    gis_copy.create_downstream_geodatasets(
                        metadata,
                        output_crs_conf=territory_configs,
                        niveau_agreg=niveau_agreg,
                        init_geometry_level=init_geometry_level,
                        dissolve_by=dissolve_by,
                        simplification=simplification,
                    )
                except Exception as exc:
                    failed.append(
                        {
                            "error": exc,
                            "aggreg": niveau_agreg,
                            "config": territory_configs,
                            "traceback": traceback.format_exc(),
                        }
                    )
                else:
                    success.append(
                        {
                            "aggreg": niveau_agreg,
                            "config": territory_configs,
                        }
                    )
    if skipped:
        for one_skipped in skipped:
            msg = "\n".join(
                [
                    "-" * 50,
                    one_skipped["warning"],
                    f"aggregation: {one_skipped['aggreg']}",
                    f"config: {one_skipped['config']}",
                ]
            )
            logger.warning(msg)
    if failed:
        for one_failed in failed:
            msg = "\n".join(
                [
                    "=" * 50,
                    f"error: {one_failed['error']}",
                    f"aggregation: {one_failed['aggreg']}",
                    f"config: {one_failed['config']}",
                    "-" * 50,
                    f"traceback:\n{one_failed['traceback']}",
                ]
            )
            logger.error(msg)

    return {
        "success": len(success),
        "skipped": len(skipped),
        "failed": len(failed),
    }


def mapshaperize_split_from_s3_multithreading(
    year: int,
    configs: List[dict],
    fs: S3FileSystem = FS,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
):

    results = {"success": 0, "skipped": 0, "failed": 0}
    if THREADS_DOWNLOAD > 1:
        with ThreadPool(min(len(configs), THREADS_DOWNLOAD)) as pool:
            args = [
                (
                    year,
                    d["mesh_init"],
                    d["source_geodata"],
                    d["simplification"],
                    d["dissolve_by"],
                    d["config"],
                    fs,
                    bucket,
                    path_within_bucket,
                )
                for d in configs
            ]
            iterator = pool.map(
                mapshaperize_split_from_s3, *zip(*args), timeout=60 * 10
            ).result()

            failed = False
            index = 0
            while True:
                try:
                    this_result = next(iterator)
                except StopIteration:
                    break
                except Exception:
                    logger.error(traceback.format_exc())
                    logger.error("args were %s", args[index])
                else:
                    for key in "success", "skipped", "failed":
                        results[key] += this_result[key]
                finally:
                    index += 1
    else:
        for d in configs:
            d["init_geometry_level"] = d.pop("mesh_init")
            d["source"] = d.pop("source_geodata")
            d["config_generation"] = d.pop("config")
            try:
                this_result = mapshaperize_split_from_s3(
                    year=year,
                    fs=fs,
                    bucket=bucket,
                    path_within_bucket=path_within_bucket,
                    **d,
                )
            except Exception:
                logger.error(traceback.format_exc())
                logger.error("args were %s", d)
            else:
                for key in "success", "skipped", "failed":
                    results[key] += this_result[key]

    skipped = results["skipped"]
    success = results["success"]
    failed = results["failed"]

    logger.info("%s file(s) generation(s) were skipped", skipped)
    logger.info("%s file(s) generation(s) succeeded", success)
    logger.error("%s file(s) generation(s) failed", failed)
    
    if failed:
        raise ValueError("some datasets' generation failed")


# if __name__ == "__main__":
#     import logging
#     from cartiflette.pipeline_constants import COG_TERRITOIRE
#     from cartiflette.config import DATASETS_HIGH_RESOLUTION

#     logging.basicConfig(level=logging.INFO)

#     mapshaperize_split_from_s3(
#         year=2023,
#         init_geometry_level="ARRONDISSEMENT_MUNICIPAL",
#         source=COG_TERRITOIRE[DATASETS_HIGH_RESOLUTION],
#         simplification=40,
#         dissolve_by="DEPARTEMENT",
#         config_generation={
#             "FRANCE_ENTIERE_DROM_RAPPROCHES": [
#                 {"format_output": "gpkg", "epsg": "4326"},
#                 {"format_output": "geojson", "epsg": "4326"},
#                 {"format_output": "gpkg", "epsg": "2154"},
#                 {"format_output": "geojson", "epsg": "2154"},
#             ]
#         },
#     )
