#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import traceback
from s3fs import S3FileSystem


from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
    INTERMEDIATE_FORMAT,
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
    # config, format_intermediate: str = "topojson", fs=FS
):

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
            logger.warning("-" * 50)
            logger.warning(one_skipped["warning"])
            logger.warning("aggregation: %s", one_skipped["aggreg"])
            logger.warning("config: %s", one_skipped["config"])
    if failed:
        for one_failed in failed:
            logger.error("=" * 50)
            logger.error("error: %s", one_failed["error"])
            logger.error("aggregation: %s", one_failed["aggreg"])
            logger.error("config: %s", one_failed["config"])
            logger.error("-" * 50)
            logger.error("traceback:\n%s", one_failed["traceback"])

    logger.info(
        f"{len(skipped)} file(s) generation(s) were skipped : %s",
        skipped,
    )
    logger.info(
        f"{len(success)} file(s) generation(s) succeeded : %s", success
    )
    if failed:
        raise ValueError(f"{len(failed)} file(s) generation(s) failed")

    return {"success": len(success), "skipped": len(skipped)}


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
