# -*- coding: utf-8 -*-
"""
Created on Thu Sep 21 13:12:53 2023

@author: thomas.grandjean
"""

from datetime import date
import fnmatch
import io
import json
import logging
import os
import py7zr
import re
import s3fs
import tempfile
from typing import Tuple
import zipfile

from cartiflette.utils import import_yaml_config, hash_file, deep_dict_update
import cartiflette

logger = logging.getLogger(__name__)

# TODO : check all docstrings


class Dataset:
    """
    Class representing a dataset stored in the yaml meant to be retrieved
    """

    md5 = None
    pattern = None

    def __init__(
        self,
        dataset_family: str = "ADMINEXPRESS",
        source: str = "EXPRESS-COG-TERRITOIRE",
        year: int = None,
        provider: str = "IGN",
        territory: str = None,
        bucket: str = cartiflette.BUCKET,
        path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
        fs: s3fs.S3FileSystem = cartiflette.FS,
    ):
        """
        Initialize a Dataset object.

        Parameters
        ----------
        dataset_family : str, optional
            Family desrcibed in the yaml file. The default is "ADMINEXPRESS".
        source : str, optional
            Source described in the yaml file. The default is
            "EXPRESS-COG-TERRITOIRE".
        year : int, optional
            Year described in the yaml file. The default is date.today().year.
        provider : str, optional
            Provider described in the yaml file. The default is "IGN".
        territory : str, optional
            Territory described in the yaml file. The default is None.
        bucket : str, optional
            Bucket to use. The default is BUCKET.
        path_within_bucket : str, optional
            path within bucket. The default is PATH_WITHIN_BUCKET.
        fs : s3fs.S3FileSystem, optional
            S3 file system to use. The default is FS.

        """
        if not year:
            year = date.today().year
        self.dataset_family = dataset_family
        self.source = source
        self.year = year
        self.territory = territory
        self.provider = provider
        self.config_open_data = import_yaml_config()
        self.json_md5 = f"{bucket}/{path_within_bucket}/md5.json"
        self.fs = fs

        self.sources = self.config_open_data[provider][dataset_family][source]

        self._get_last_md5()

    def __str__(self):
        dataset_family = self.dataset_family
        source = self.source
        year = self.year
        territory = self.territory
        provider = self.provider

        name = (
            f"<Dataset {provider} {dataset_family} {source} "
            f"{territory} {year}>"
        )
        return name

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def _md5(file_path: str) -> str:
        """
        Compute the md5 hash value of a file given it's path.

        Parameters
        ----------
        file_path : str
            path of the target file.

        Returns
        -------
        str
            md5 hash value

        """
        return hash_file(file_path)

    def _get_last_md5(self) -> None:
        """
        Read the last md5 hash value of the target on the s3 and store it
        as an attribute of the Dataset : self.md5
        """

        try:
            with self.fs.open(self.json_md5, "r") as f:
                all_md5 = json.load(f)
        except Exception as e:
            logger.warning(e)
            logger.warning("md5 not found")
            return
        try:
            md5 = all_md5[self.provider][self.dataset_family][self.source][
                self.territory
            ][str(self.year)]
            self.md5 = md5
        except Exception as e:
            logger.debug(e)
            logger.info("file not referenced in md5 json")

    def update_json_md5(self, md5: str) -> bool:
        "Mise à jour du json des md5"
        md5 = {
            self.provider: {
                self.dataset_family: {
                    self.source: {self.territory: {str(self.year): md5}}
                }
            }
        }
        path_filesystem = self.json_md5
        json_in_bucket = path_filesystem in self.fs.ls(
            path_filesystem.rsplit("/", maxsplit=1)[0]
        )
        try:
            if json_in_bucket:
                with self.fs.open(self.json_md5, "r") as f:
                    all_md5 = json.load(f)
                    all_md5 = deep_dict_update(all_md5, md5)
            else:
                all_md5 = md5
            with self.fs.open(self.json_md5, "w") as f:
                json.dump(all_md5, f)
            return True
        except Exception as e:
            logger.warning(e)
            logger.warning("md5 not written")
            return False

    def get_path_from_provider(self) -> str:
        """
        Get the path to download the file from (based on the yaml content)

        Raises
        ------
        ValueError
            If the input (self.provider, self.year, ...) are not matching the
            content of the yaml file.

        Returns
        -------
        str
            path to download the file from

        """

        provider = self.provider
        dataset_family = self.dataset_family
        year = self.year
        source = self.source
        sources = self.sources

        # print(sources)

        exclude = {"territory", "FTP"}
        available_years = set(sources.keys()) - exclude

        if year is None:
            year = max(available_years)
            msg = (
                f"year {year} is latest available for {provider} "
                f"with source {source}"
            )
            logger.info(msg)

        if year not in available_years:
            msg = (
                f"year {year} not available for provider {provider} "
                f"with source {source}"
            )
            raise ValueError(msg)

        # Let's scroll the yaml to find first `file` (fixed URL) or
        # structure` value (url to be formatted with diverse fields)
        # and pattern

        d = self.config_open_data.copy()
        for key in provider, dataset_family, source, year:
            d = d[key]
            try:
                self.pattern = d["pattern"]
                break
            except KeyError:
                continue

        d = self.config_open_data.copy()
        for key in provider, dataset_family, source, year:
            d = d[key]
            try:
                url = d["file"]
                break
            except KeyError:
                continue

        try:
            url
        except UnboundLocalError:
            d = self.config_open_data.copy()
            for key in provider, dataset_family, source, year:
                d = d[key]
                try:
                    url = d["structure"]
                    break
                except KeyError:
                    continue

            if not url:
                msg = (
                    "Neither `file` nor `structure` has been found in the "
                    f"yaml file for {self}"
                )
                raise ValueError(msg)

            if self.territory:
                try:
                    territories = sources["territory"]
                except KeyError:
                    # No territory
                    territory = ""
                else:
                    try:
                        territory = territories[self.territory]
                    except KeyError:
                        msg = (
                            f"error on territory : {self.territory} "
                            f"not in {territories}"
                        )
                        raise ValueError(msg)

            kwargs = sources[year].copy()
            try:
                kwargs["territory"] = territory
            except UnboundLocalError:
                pass

            url = url.format(**kwargs)

        logger.info(f"using {url}")

        return url

    def set_temp_file_path(self, path: str) -> None:
        """
        Store a given path as self.temp_archive_path to retrieve it later.

        Parameters
        ----------
        path : str
            Path to store

        Returns
        -------
        None
        """
        self.temp_archive_path = path

    def unpack(self, protocol: str) -> Tuple[str, Tuple[str, ...]]:
        """
        Decompress a group of files if they validate a pattern and an extension
        type. Returns the path to the folder containing
        the decompressed files. Note that this folder will be stored in the
        temporary cache, but requires manual cleanup.
        If nested archives (ie zip in zip), will unpack all nested data and
        look for target pattern **INSIDE** the nested archive only

        Parameters
        ----------
        protocol: str
            Protocol to use for unpacking. Either "7z" or "zip"

        Raises
        ------
        ValueError
            If protocol not "7z" or "zip"

        Returns
        -------
        Tuple[str, Tuple[str, ...]]
            First element is the root folder to cleanup
            Second element is a tuple of subdirectories containing targeted
            files (unzipped)

        """
        if protocol not in {"7z", "zip"}:
            raise ValueError(f"Unknown protocol {protocol}")

        # unzip in temp directory
        location = tempfile.mkdtemp()
        logger.debug(f"Extracting to {location}")

        year = self.year
        source = self.source
        territory = self.territory
        sources = self.sources

        def filter_case_insensitive(fnmatch_pattern: str, x: list) -> list:
            # glob and fnmatch are case sensitive on linux platforms,
            # so convert pattern to regex
            pattern = fnmatch.translate(fnmatch_pattern)
            pattern = re.compile(pattern, flags=re.IGNORECASE)
            return [y for y in x if pattern.match(y)]

        def get_utils_from_protocol(protocol):
            if protocol == "7z":
                loader = py7zr.SevenZipFile
                list_files = "getnames"
                extract = "extract"
                targets_kw = "targets"
            else:
                loader = zipfile.ZipFile
                list_files = "namelist"
                extract = "extractall"
                targets_kw = "members"
            return loader, list_files, extract, targets_kw

        extracted = []
        archives_to_process = [(self.temp_archive_path, protocol)]
        while archives_to_process:
            archive, protocol = archives_to_process.pop()
            loader, list_files, extract, targets_kw = get_utils_from_protocol(
                protocol
            )
            with loader(archive, mode="r") as archive:
                files = getattr(archive, list_files)()

                # Handle nested archives (and presume there is no mixup in
                # formats...)
                archives = [
                    x for x in files if x.endswith(".zip") or x.endswith(".7z")
                ]
                archives = [(x, x.split(".")[-1]) for x in archives]
                for nested_archive, protocol in archives:
                    with archive.open(nested_archive) as nested:
                        archives_to_process.append(
                            (io.BytesIO(nested.read()), protocol)
                        )

                files = filter_case_insensitive(self.pattern, files)

                if year <= 2020 and source.endswith("-TERRITOIRE"):
                    territory_code = sources["territory"][territory].split(
                        "_"
                    )[0]
                    files = {x for x in files if territory_code in x}

                # Find all auxiliary files sharing the same name as those found
                # (needed for shapefiles)
                if any(x.lower().endswith(".shp") for x in files):
                    shapefiles_pattern = {
                        os.path.splitext(x)[0]
                        for x in files
                        if x.lower().endswith(".shp")
                    }
                else:
                    shapefiles_pattern = set()

                if shapefiles_pattern:
                    targets = [
                        x
                        for x in getattr(archive, list_files)()
                        if os.path.splitext(x)[0] in shapefiles_pattern
                    ]
                else:
                    targets = files

                logger.debug(targets)
                logger.debug(len(targets))
                kwargs = {"path": location, targets_kw: targets}
                getattr(archive, extract)(**kwargs)
                extracted += [
                    os.path.join(location, target) for target in targets
                ]

        # paths = {os.path.dirname(x) for x in extracted}
        paths = tuple(extracted)
        self._list_levels(paths)

        root_cleanup = location
        return root_cleanup, paths

    def _list_levels(self, extracted: tuple) -> None:
        """
        List available levels in a raw dataset. Will have unexpected results on
        any dataset which is not from an IGN source. The list consists of each
        shapefile's basename in any case.
        The list itsself is stored in self.levels.

        Parameters
        ----------
        extracted : tuple
            Paths to extracted shapefiles (and auxilary files).

        Returns
        -------
        None

        """

        levels = [
            os.path.splitext(os.path.basename(file))[0]
            for file in extracted
            if os.path.splitext(file)[-1].lower() == ".shp"
        ]

        logger.info(
            "\n  - ".join(["Available administrative levels are :"] + levels)
        )
        self.levels = levels
