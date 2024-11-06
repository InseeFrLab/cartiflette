#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classe générique pour travailler autour d'un dataset présent sur le S3
"""

import logging
import os
import shutil
import tempfile
from typing import List
import warnings

from diskcache import Cache
import pandas as pd
from s3fs import S3FileSystem
from retrying import retry


from cartiflette.config import FS, RETRYING
from cartiflette.utils import (
    create_path_bucket,
    ConfigDict,
    DICT_CORRESP_ADMINEXPRESS,
)

logger = logging.getLogger(__name__)
cache = Cache("cartiflette-s3-cache", timeout=3600)

if not RETRYING:
    # patch retrying
    def retry(*args, **kwargs):
        def decorator(func):
            return func

        return decorator


class S3Dataset:
    """
    Base class representing a dataset stored on the S3

    This class is used on it's own only for tabular datasets (to be joined to
    S3GeoDataset for enrichment)
    """

    files = None
    main_filename = None
    s3_dirpath = None
    local_dir = None

    def __init__(
        self,
        fs: S3FileSystem = FS,
        filename: str = "*",
        build_from_local: str = None,
        **config: ConfigDict,
    ):
        """
        Create a S3Dataset.

        Parameters
        ----------
        fs : S3FileSystem, optional
            S3FileSystem used for storage. The default is FS.
        filename : str, optional
            In case there are multiple files into the same folder define it
            to avoid catching the wrong file from S3FileSystem
            (this should only occur with the download of raw datasets with
             COMMUNE.shp and ARRONDISSEMENT_MUNICIPAL.shp being stored in the
             same directory).
            The default is "*".
            For instance, "COMMUNE.shp"
        build_from_local : str, optional
            If the object is generated from local files, should be the path
            to the main file of the dataset.
            If None, the path will be deduced from the S3 and the main filename
            also.
        **config : ConfigDict
            Other arguments to define the path on the S3 to the dataset.
        """
        self.fs = fs
        self.config = config
        self.build_from_local = build_from_local
        self.local_files = []

        self.filename = filename.rsplit(".", maxsplit=1)[0]

        self.source = (
            f"{config.get('provider', '')}:{config.get('source', '')}"
        )

        if build_from_local and not os.path.exists(build_from_local):
            raise ValueError(f"File not found at {build_from_local}")

        self.get_path_of_dataset()

    def __str__(self):
        return f"<cartiflette.s3.dataset.S3Dataset({self.config})>"

    def __repr__(self):
        return self.__str__()

    def __enter__(self):
        "download file into local folder at enter"
        if not self.build_from_local:
            self.local_dir = tempfile.mkdtemp()
            self.to_local_folder_for_mapshaper()
        return self

    def _get_columns(self, **kwargs):
        "return the current dataset's columns"
        df = self.to_frame(**kwargs, nrows=5)
        return df.columns.tolist()

    @staticmethod
    def find_column_name(column: str, columns: List[str]) -> str:
        """
        Retrieve a column's full name among available columns, using a
        compiled regex expression from DICT_CORRESP_ADMINEXPRESS.

        Parameters
        ----------
        column : str
            The searched column
        columns : List[str]
            The list of columns to search into.

        Raises
        ------
        ValueError
            If the searched column corresponds to more than one result.
        IndexError
            If the searched column is not found.


        Returns
        -------
        str
            The column's full name.

        """
        compiled = DICT_CORRESP_ADMINEXPRESS[column]
        founds = [col for col in columns if compiled.match(col)]
        if len(founds) > 1:
            raise ValueError(f"{column=} matched multiple columns : {founds=}")
        try:
            return founds[0]
        except IndexError as exc:
            raise IndexError(
                f"{column=}/{compiled=} not found among {columns=}"
            ) from exc

    def to_frame(self, **kwargs) -> pd.DataFrame:
        return pd.read_csv(
            os.path.join(self.local_dir, self.main_filename), **kwargs
        )

    def clean(self):
        "remove files from local dir"
        try:
            try:
                shutil.rmtree(self.local_dir)
            except FileNotFoundError:
                pass
        except Exception as exc:
            warnings.warn(exc)

    def __exit__(self, *args, **kwargs):
        "remove tempfiles at exit"
        self.clean()

    def get_path_of_dataset(self):
        "retrieve dataset's full paths on S3"
        path = os.path.dirname(create_path_bucket(self.config))
        search = f"{path}/**/{self.filename}"
        if self.filename != "*":
            search += ".*"

        self.s3_files = self.fs.glob(search)

        if self.build_from_local:
            # This S3Dataset has been created from a local file
            self.s3_dirpath = path
            self.local_dir = os.path.dirname(self.build_from_local)
            self.main_filename = os.path.basename(self.build_from_local)

            return

        if not self.s3_files:
            raise ValueError(
                f"this dataset is not available on S3 on {search}"
            )

        if len(self.s3_files) > 1:
            main_filename = (
                self.s3_files[0].rsplit(".", maxsplit=1)[0] + ".shp"
            )
        else:
            main_filename = self.s3_files[0]

        self.main_filename = os.path.basename(main_filename)
        self.s3_dirpath = os.path.dirname(main_filename)

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def to_s3(self):
        "upload file to S3"
        target = self.s3_dirpath
        if not target.endswith("/"):
            target += "/"
        logger.debug("sending %s -> %s", self.local_dir, target)
        self.fs.put(self.local_dir + "/*", target, recursive=True)

    def _read(self, src: str) -> bytes:
        """
        Read bytes from a file on S3FileSystem with disk cache support

        Parameters
        ----------
        src : str
            Source of file

        Returns
        -------
        bytes
            File content

        """
        try:
            return cache[src]
        except KeyError:
            with self.fs.open(src, "rb") as f:
                content = f.read()
            cache[src] = content
        return content

    def download(self, src: str, dest: str):
        """
        Download a file from S3FileSystem to localdir with cache support

        Parameters
        ----------
        src : str
            Path of source file on S3FileSystem
        dest : str
            Path to write the file's content on local directory.

        Returns
        -------
        None.

        """
        "download to dest with disk cache"
        content = self._read(src)
        with open(dest, "wb") as f:
            f.write(content)

    def to_local_folder_for_mapshaper(self):
        "download to local dir and prepare for use with mapshaper"

        if not self.s3_files:
            raise ValueError(
                f"this dataset is not available on S3 : {self.s3_dirpath}"
            )

        files = []

        # Get all files (plural in case of shapefile) from Minio
        logger.debug("downloading %s to %s", self.s3_files, self.local_dir)
        for file in self.s3_files:
            path = f"{self.local_dir}/{file.rsplit('/', maxsplit=1)[-1]}"
            self.download(file, path)
            logger.info("file written to %s", path)
            files.append(path)

        self.local_files = files

    def update_s3_path_evaluation(self):
        path = os.path.dirname(create_path_bucket(self.config))
        self.s3_dirpath = path
