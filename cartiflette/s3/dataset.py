#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classe générique pour travailler autour d'un dataset présent sur le S3
"""

import logging
import os
from s3fs import S3FileSystem
import shutil

from cartiflette.config import FS
from cartiflette.utils import create_path_bucket, ConfigDict
from cartiflette.pipeline.prepare_mapshaper import (
    prepare_local_directory_mapshaper,
)
from cartiflette.mapshaper import mapshaper_convert_mercator

logger = logging.getLogger(__name__)


class BaseGISDataset:
    files = None

    def __init__(
        self,
        fs: S3FileSystem = FS,
        intermediate_dir: str = "temp",
        **config: ConfigDict,
    ):
        self.fs = fs
        self.config = config
        self.s3_dirpath = self.get_path_of_dataset()
        self.local_dir = intermediate_dir

    def get_path_of_dataset(self):
        "retrieve dataset's full path (including auxiliary files for shp)"
        path = os.path.dirname(create_path_bucket(self.config))
        search = os.path.join(path, "**/*")
        self.files = self.fs.glob(search)
        if not self.files:
            raise ValueError("this dataset is not available")

        # return exact path (without glob expression):
        return os.path.dirname(self.files[0])

    def to_local_folder_for_mapshaper(self):
        "download to local dir and prepare for use with mapshaper"
        paths = prepare_local_directory_mapshaper(
            self.s3_dirpath,
            borders="COMMUNE",
            territory=self.config["territory"],
            niveau_agreg="COMMUNE",
            format_output="geojson",
            simplification=0,
            local_dir=self.local_dir,
            fs=self.fs,
        )
        logger.warning(paths)

    def __enter__(self):
        self.to_local_folder_for_mapshaper()
        return self

    def __exit__(self, *args, **kwargs):
        shutil.rmtree(os.path.join(self.local_dir, self.config["territory"]))
        pass

    def to_mercator(self, format_intermediate: str = "geojson"):
        "project to mercator using mapshaper"
        mapshaper_convert_mercator(
            local_dir=self.local_dir,
            territory=self.config["territory"],
            identifier=self.config["territory"],
            format_intermediate=format_intermediate,
        )


# if __name__ == "__main__":
#     with BaseGISDataset(
#         bucket=BUCKET,
#         path_within_bucket=PATH_WITHIN_BUCKET,
#         provider="IGN",
#         dataset_family="ADMINEXPRESS",
#         source="EXPRESS-COG-TERRITOIRE",
#         year=2024,
#         borders=None,
#         crs="*",
#         filter_by="origin",
#         value="raw",
#         vectorfile_format="shp",
#         territory="mayotte",
#         simplification=0,
#     ) as dset:
#         dset.to_mercator()
