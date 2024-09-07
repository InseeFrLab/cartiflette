#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classe générique pour travailler autour d'un dataset présent sur le S3
"""


from glob import glob
import logging
import os
import shutil
import warnings

from s3fs import S3FileSystem


from cartiflette.config import FS
from cartiflette.utils import (
    create_path_bucket,
    ConfigDict,
)


class S3Dataset:
    """
    Base class representing a dataset stored on the S3

    This class is used on it's own only for tabular datasets (to be joined to
    S3GeoDataset for enrichment)
    """

    files = None
    main_filename = None
    s3_dirpath = None

    def __init__(
        self,
        fs: S3FileSystem = FS,
        local_dir: str = "temp",
        filename: str = "*",
        **config: ConfigDict,
    ):
        """
        Create a S3Dataset.

        Parameters
        ----------
        fs : S3FileSystem, optional
            S3FileSystem used for storage. The default is FS.
        local_dir : str, optional
            Local directory used for transformations using mapshaper. The
            default is "temp".
        filename : str, optional
            In case there are multiple files into the same folder define it
            to avoid catching the wrong file from S3FileSystem
            (this should only occur with the download of raw datasets with
             COMMUNE.shp and ARRONDISSEMENT_MUNICIPAL.shp being stored in the
             same directory).
            The default is "*".
            For instance, "COMMUNE.shp"
        **config : ConfigDict
            Other arguments to define the path on the S3 to the dataset.
        """
        self.fs = fs
        self.config = config
        self.local_dir = local_dir
        self.local_files = []

        self.filename = filename.rsplit(".", maxsplit=1)[0]

        self.get_path_of_dataset()

        self.source = (
            f"{config.get('provider', '')}:{config.get('source', '')}"
        )

    def __str__(self):
        return f"<cartiflette.s3.dataset.S3Dataset({self.config})>"

    def __repr__(self):
        return self.__str__()

    def get_path_of_dataset(self):
        "retrieve dataset's full paths on S3"
        path = os.path.dirname(create_path_bucket(self.config))
        search = f"{path}/**/{self.filename}"
        if self.filename != "*":
            search += ".*"

        self.s3_files = self.fs.glob(search)
        if not self.s3_files:
            warnings.warn(f"this dataset is not available on S3 on {search}")

            self.s3_dirpath = path

            # This S3Dataset should have been created from a local file, try
            # to find the main file from self.localdir
            files = glob(
                f"{self.local_dir}/*.{self.config['vectorfile_format']}"
            )
            try:
                self.main_filename = os.path.basename(files[0])
            except KeyError as exc:
                raise ValueError(
                    "this dataset has neither been found on localdir nor on S3"
                ) from exc
            return

        if len(self.s3_files) > 1:
            main_filename = (
                self.s3_files[0].rsplit(".", maxsplit=1)[0] + ".shp"
            )
        else:
            main_filename = self.s3_files[0]

        self.main_filename = os.path.basename(main_filename)
        self.s3_dirpath = os.path.dirname(main_filename)

    def to_s3(self):
        "upload file to S3"
        target = self.s3_dirpath
        if not target.endswith("/"):
            target += "/"
        logging.info("sending %s -> %s", self.local_dir, target)
        self.fs.put(self.local_dir + "/*", target, recursive=True)

    def to_local_folder_for_mapshaper(self):
        "download to local dir and prepare for use with mapshaper"

        if not self.s3_files:
            raise ValueError(
                f"this dataset is not available on S3 : {self.s3_dirpath}"
            )

        self.local_dir = f"{self.local_dir}/{self.config['territory']}"
        os.makedirs(self.local_dir, exist_ok=True)

        files = []

        # Get all files (plural in case of shapefile) from Minio
        logging.info("downloading %s to %s", self.s3_files, self.local_dir)
        for file in self.s3_files:
            path = f"{self.local_dir}/{file.rsplit('/', maxsplit=1)[-1]}"
            self.fs.download(file, path)
            logging.info("file written to %s", path)
            files.append(path)

        self.local_files = files

    def __enter__(self):
        "download file into local folder at enter"
        self.to_local_folder_for_mapshaper()
        return self

    def __exit__(self, *args, **kwargs):
        "remove tempfiles as exit"
        try:
            try:
                shutil.rmtree(os.path.join(self.local_dir))
            except FileNotFoundError:
                pass
        except Exception as exc:
            warnings.warn(exc)
