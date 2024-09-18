#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from s3fs import S3FileSystem


from cartiflette.config import (
    BUCKET,
    PATH_WITHIN_BUCKET,
    FS,
    INTERMEDIATE_FORMAT,
)
from cartiflette.s3 import S3GeoDataset, S3Dataset


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
        **kwargs,
    ) as gis_file:

        for crs, crs_configs in config_generation.items():
            for config_one_file in crs_configs:

                with gis_file.copy() as gis_copy:
                    gis_copy.create_downstream_geodatasets(
                        metadata,
                        format_output=config_one_file["format"],
                        niveau_agreg=config_one_file["territory"],
                        init_geometry_level=init_geometry_level,
                        dissolve_by=dissolve_by,
                        crs=crs,
                        simplification=simplification,
                    )


# if __name__ == "__main__":
#     import logging

#     logging.basicConfig(level=logging.INFO)

#     # mapshaperize_split_from_s3(
#     #     year=2024,
#     #     init_geometry_level="CANTON",
#     #     source="EXPRESS-COG-CARTO-TERRITOIRE",
#     #     simplification=40,
#     #     dissolve_by="CANTON",
#     #     config_generation={
#     #         "4326": [
#     #             {"territory": "TERRITOIRE", "format": "topojson"},
#     #             {"territory": "TERRITOIRE", "format": "gpkg"},
#     #             {"territory": "TERRITOIRE", "format": "geojson"},
#     #             {"territory": "REGION", "format": "topojson"},
#     #             {"territory": "REGION", "format": "gpkg"},
#     #             {"territory": "REGION", "format": "geojson"},
#     #             {
#     #                 "territory": "FRANCE_ENTIERE_DROM_RAPPROCHES",
#     #                 "format": "topojson",
#     #             },
#     #             {
#     #                 "territory": "FRANCE_ENTIERE_DROM_RAPPROCHES",
#     #                 "format": "gpkg",
#     #             },
#     #             {
#     #                 "territory": "FRANCE_ENTIERE_DROM_RAPPROCHES",
#     #                 "format": "geojson",
#     #             },
#     #             {"territory": "FRANCE_ENTIERE", "format": "topojson"},
#     #             {"territory": "FRANCE_ENTIERE", "format": "gpkg"},
#     #             {"territory": "FRANCE_ENTIERE", "format": "geojson"},
#     #             {"territory": "DEPARTEMENT", "format": "topojson"},
#     #             {"territory": "DEPARTEMENT", "format": "gpkg"},
#     #             {"territory": "DEPARTEMENT", "format": "geojson"},
#     #         ]
#     #     },
#     # )
