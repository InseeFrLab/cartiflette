# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 20:52:51 2023

@author: thomas.grandjean
"""

import requests
from tqdm import tqdm
import os
import geopandas as gpd
import logging
from datetime import date
import s3fs
import json
from shapely.validation import make_valid
import py7zr
from itertools import product
import ftplib
from typing import TypedDict
from glob import glob
import tempfile
import numpy as np

from cartiflette.utils import import_yaml_config, hash_file, deep_dict_update

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "diffusion/shapefiles-test1"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

BASE_CACHE_PATTERN = os.path.join("**", "*DONNEES_LIVRAISON*", "**")

kwargs = {}
for key in ["token", "secret", "key"]:
    try:
        kwargs[key] = os.environ[key]
    except KeyError:
        continue
FS = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL}, **kwargs)


logger = logging.getLogger(__name__)


class Dataset:
    """
    Class representing a dataset stored in the yaml meant to be retrieved
    """

    md5 = None

    def __init__(
        self,
        dataset_family: str = "ADMINEXPRESS",
        source: str = "EXPRESS-COG-TERRITOIRE",
        year: int = date.today().year,
        provider: str = "IGN",
        territory: str = None,
        bucket: str = BUCKET,
        path_within_bucket: str = PATH_WITHIN_BUCKET,
        fs: s3fs.S3FileSystem = FS,
    ):
        """
        Initialize a Dataset object.

        Parameters
        ----------
        dataset_family : str, optional
            Family descibed in the yaml file. The default is "ADMINEXPRESS".
        source : str, optional
            Source descibed in the yaml file. The default is "EXPRESS-COG-TERRITOIRE".
        year : int, optional
            Year descibed in the yaml file. The default is date.today().year.
        provider : str, optional
            Provider descibed in the yaml file. The default is "IGN".
        territory : str, optional
            Territory descibed in the yaml file. The default is None.
        bucket : str, optional
            Bucket to use. The default is BUCKET.
        path_within_bucket : str, optional
            path within bucket. The default is PATH_WITHIN_BUCKET.
        fs : s3fs.S3FileSystem, optional
            S3 file system to use. The default is FS.

        """
        self.dataset_family = dataset_family
        self.source = source
        self.year = year
        self.territory = territory
        self.provider = provider
        self.config_open_data = import_yaml_config()
        self.json_md5 = f"{bucket}/{path_within_bucket}/md5.json"
        self.fs = fs

        self.sources = self.config_open_data[provider][dataset_family][source]

        self.__get_last_md5__()

    def __str__(self):
        dataset_family = self.dataset_family
        source = self.source
        year = self.year
        territory = self.territory
        provider = self.provider

        name = f"<Dataset {provider} {dataset_family} {source} {territory} {year}>"
        return name

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def __md5__(file_path: str) -> str:
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

    def __get_last_md5__(self) -> None:
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

        md5 = all_md5[self.provider][self.dataset_family][self.source][
            self.territory
        ][str(self.year)]
        self.md5 = md5

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
        year = self.year
        source = self.source
        sources = self.sources

        exclude = {"field", "FTP"}
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

        try:
            url = sources[year]["file"]

        except KeyError:
            field = sources["field"][self.territory]
            structure = sources[year]["structure"]

            kwargs = sources[year].copy()
            kwargs["field"] = field
            del kwargs["structure"]

            url = structure.format(**kwargs)

        logger.info(f"using {url}")

        return url

    @staticmethod
    def __sanitize_file__(file_path: str, preserve: str = "shape") -> None:
        """
        Sanitizes geometries of geodataframe from file (if needed).
        Overwrites the file in place if unvalid geometries are detected.

        Parameters
        ----------
        url : str
            path to the source file
        preserve : str, optional
            choose which characteristic to preserve while sanitizing geometries
            (either preserve topology or shape)

        Raises
        ------
        ValueError
            If the GeoDataFrame contains missing geometries or if the
            sanitation fails

        Returns
        -------
        None

        """
        logger.debug(f"sanitizing geometries of {file_path}")

        if preserve not in ["shape", "topology"]:
            msg = (
                "preserve must be either of 'shape', 'topology' - found "
                f"'{preserve}' instead"
            )
            raise ValueError(msg)

        gdf = gpd.read_file(file_path)
        geom = gdf.geometry
        ix = geom[(geom.is_empty | geom.isna())].index
        if len(ix) > 0:
            raise ValueError("Geometries are missing")

        ix = geom[~(geom.is_empty | geom.isna()) & ~geom.is_valid].index
        if len(ix) > 0:
            geoms = gdf.loc[ix, "geometry"]
            if preserve == "shape":
                gdf.loc[ix, "geometry"] = geoms.apply(make_valid)
            elif preserve == "topology":
                gdf.loc[ix, "geometry"] = geoms.buffer(0)

            gdf.to_file(file_path)

        ix = geom[~(geom.is_empty | geom.isna()) & ~geom.is_valid].index
        if len(ix) > 0:
            msg = "Unvalid geometries are still present in the file"
            raise ValueError(msg)

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

    def unzip(
        self,
        pattern: str = BASE_CACHE_PATTERN,
        preserve: str = "shape",
        ext: str = ".shp",
    ) -> str:
        """
        Decompress a group of files if they validate a pattern and an extension
        type, sanitize geometries. Returns the path to the folder containing
        the decompressed files. Note that this folder will be stored in the
        temporary cache, but requires manual cleanup.

        Parameters
        ----------
        pattern : str, optional
            Pattern to validate. The default is BASE_CACHE_PATTERN.
        preserve : str, optional
            Specify which geometric option should be preserved while doing
            geometry sanitation (either shape or topology).
            The default is "shape".
        ext : str, optional
            File extension to look for (.shp, .gpkg, etc.). In order to
            preserve all auxiliary file (in case of shapefile for instance:
            .prj, .dbf, ...) all files sharing the same name (but it's
            extension) as one validated will be decompressed as well.
            The default is .shp.

        Returns
        -------
        str
            path to unzipped files

        """

        if preserve not in ["shape", "topology"]:
            msg = (
                "preserve must be either of 'shape', 'topology' - found "
                f"'{preserve}' instead"
            )
            raise ValueError(msg)

        if ext is not None:
            ext = ext.lower()

        # unzip in temp directory
        location = tempfile.mkdtemp()
        logger.debug(f"Extracting to {location}")

        year = self.year
        source = self.source
        territory = self.territory
        sources = self.sources

        # TODO : tester le type de fichier au préalable ?
        # (-> choix py7zr, zipfile, etc.)

        with py7zr.SevenZipFile(self.temp_archive_path, mode="r") as archive:
            files = archive.getnames()
            shapefiles = {
                os.path.splitext(x)[0]
                for x in files
                if x.lower().endswith(ext)
            }

            if year <= 2020 and source.endswith("-TERRITOIRE"):
                field_code = sources["field"][territory].split("_")[0]
                shapefiles = {x for x in shapefiles if field_code in x}

            logger.debug(shapefiles)

            # Find all auxiliary files sharing the same name as those found :
            targets = [
                x for x in files if os.path.splitext(x)[0] in shapefiles
            ]
            logger.debug(targets)
            logger.debug(len(targets))
            archive.extract(path=location, targets=targets)

        # note that glob is case sensitive on linux
        combinations_patterns = [
            [pattern.lower(), pattern.upper()],
            [f"*{ext}", f"*{ext.upper()}"],
        ]

        patterns = list(product(*combinations_patterns))
        paths = []
        for list_pattern in patterns:
            pattern = os.path.join(*list_pattern)
            paths += glob(os.path.join(location, pattern), recursive=True)
        logger.debug(paths)

        # TODO : envisager un multiprocessing ?
        for file in paths:
            self.__sanitize_file__(file, preserve=preserve)

        paths = {os.path.dirname(x) for x in paths}
        shp_location = tuple(paths)
        return shp_location


class BaseScraper:
    """
    Base scraper. Not meant to be used by itself, but only when surcharged.
    """

    @staticmethod
    def __validate_file__(file_path: str, hash):
        """
        https://gist.github.com/mjohnsullivan/9322154
        Validates a file against an MD5 hash value
        :param file_path: path to the file for hash validation
        :type file_path:  string
        :param hash:      expected hash value of the file
        :type hash:       string -- MD5 hash value
        """
        return hash_file(file_path) == hash


class HttpScraper(BaseScraper, requests.Session):
    """
    Scraper with specific download method for http/https get protocol. Not
    meant to be used by itself, but only when surcharged.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize HttpScraper and set eventual proxies from os environment
        variables. *args and **kwargs are arguments that should be processed
        by a requests.Session object.

        Parameters
        ----------
        *args :
            Arguments passed to requests.Session
        **kwargs :
            Arguments passed to requests.Session.

        """
        super().__init__(*args, **kwargs)

        for protocol in ["http", "https"]:
            try:
                proxy = {protocol: os.environ[f"{protocol}_proxy"]}
                self.proxies.update(proxy)
            except KeyError:
                continue

    def download_to_tempfile_http(
        self, url: str, hash: str = None, **kwargs
    ) -> tuple[bool, str]:
        """
        Performs a HTTP(S) download that will ensure file integrity (through
        md5 hash signature or file's length if available) and that
        the file is a new one (if a previous md5 signature has been given)

        The file will be written on a temporary file.

        If the file has been updated, the first element of the tuple will be
        True (False otherwise). If True, the path to the temporary file will be
        returned as a second element

        Parameters
        ----------
        url : str
            url to download the file from
        hash : str, optional
            previous hash signature of the file at latest download. The default
            is None.
        **kwargs :
            Additional kwargs are passed to requests.get (though any "stream"
            value will be ignored)

        Raises
        ------
        IOError
            If validation of downloaded file fails (through expected
            md5-content or Content-length response headers)

        Returns
        -------
        tuple[bool, str]
            bool : True if a new file has been downloaded, False in other cases
            str : path to the temporary file if bool was True (else None)
        """

        # ignore kwargs["stream"] if it is passed in kwargs
        try:
            del kwargs["stream"]
        except KeyError:
            pass

        block_size = 1024 * 1024  # 1MiB

        # check file's characteristics
        head = super().head(url, **kwargs).headers
        try:
            expected_md5 = head["content-md5"]
            logger.debug(f"File MD5 is {expected_md5}")
        except KeyError:
            expected_md5 = None
            logger.warning(f"md5 not found in header at url {url}")
        else:
            if hash and expected_md5 == hash:
                # unchanged file -> exit
                logger.info(f"md5 matched at {url} - download cancelled")
                return False, None
        finally:
            try:
                # No MD5 in header -> check requested file's size
                expected_file_size = int(head["Content-length"])
                logger.debug(f"File size is {expected_file_size}")
            except KeyError:
                expected_file_size = None
                msg = f"Content-Length not found in header at url {url}"
                logger.warning(msg)

        with tempfile.NamedTemporaryFile("wb", delete=False) as temp_file:
            file_path = temp_file.name
            logger.debug(f"Downloading to {file_path}")

            logger.debug(f"starting download at {url}")
            r = super().get(url, stream=True, **kwargs)
            if not r.ok:
                raise IOError(f"download failed with {r.status_code} code")
            with tqdm(
                desc="Downloading: ",
                total=int(np.ceil(expected_file_size / block_size)),
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                leave=False,
            ) as pbar:
                for chunk in r.iter_content(chunk_size=block_size):
                    if chunk:  # filter out keep-alive new chunks
                        size = temp_file.write(chunk)
                        pbar.update(size)

        # Check that the downloaded file has the expected characteristics
        if expected_md5:
            if not self.__validate_file__(file_path, expected_md5):
                raise IOError("download failed (corrupted file)")
        elif expected_file_size:
            # check that the downloaded file is the expected size
            if not expected_file_size == os.path.getsize(file_path):
                raise IOError("download failed (corrupted file)")

        # if there's a hash value, check if there are any changes
        if hash and self.__validate_file__(file_path, hash):
            # unchanged file -> exit (after deleting the downloaded file)
            logger.info(f"md5 matched at {url} after download")
            os.unlink(file_path)
            return False, None

        return True, file_path


class FtpScraper(BaseScraper, ftplib.FTP):
    """
    Scraper with specific download method for ftp protocol. Not meant to be
    used by itself, but only when surcharged.
    """

    def download_to_tempfile_ftp(
        self, url: str, hash: str = None, **kwargs
    ) -> tuple[bool, str]:
        """
        Performs a FTP download that will ensure file integrity (through
        the file's length) and that the file is a new one (if a previous md5
        signature has been given)

        The file will be written on a temporary file.

        If the file has been updated, the first element of the tuple will be
        True (False otherwise). If True, the path to the temporary file will be
        returned as a second element

        Parameters
        ----------
        url : str
            url to download the file from
        hash : str, optional
            previous hash signature of the file at latest download. The default
            is None.
        **kwargs :
            Will be ignored (set for consistency with HttpScraper)

        Raises
        ------
        IOError
            If validation of downloaded file fails

        Returns
        -------
        tuple[bool, str]
            bool : True if a new file has been downloaded, False in other cases
            str : path to the temporary file if bool was True (else None)
        """

        temp_file = tempfile.NamedTemporaryFile()
        file_path = temp_file.name + ".7z"
        logger.debug(f"Downloading to {file_path}")

        expected_file_size = self.size(url)
        logger.debug(f"File size is {expected_file_size}")

        logger.debug(f"starting download at {url}")
        with open(file_path, "wb") as file:
            with tqdm(
                desc="Downloading: ",
                total=expected_file_size,
                unit_scale=True,
                miniters=1,
                leave=True,
            ) as pbar:

                def dowload_write(data):
                    pbar.update(len(data))
                    file.write(data)

                self.retrbinary(f"RETR {url}", dowload_write)

        # check that the downloaded file is the expected size
        if not expected_file_size == os.path.getsize(file_path):
            raise IOError("download failed (corrupted file)")

        # if there's a hash value, check if there are any changes
        if hash and self.__validate_file__(file_path, hash):
            # unchanged file -> exit (after deleting the downloaded file)
            os.unlink(file_path)
            return False, None


class MasterScraper(HttpScraper, FtpScraper):
    """
    Scraper main class which could be used to perform either http/https get
    downloads of ftp downloads.
    """

    class DownloadReturn(TypedDict):
        downloaded: bool
        hash: str
        path: str

    def download_unzip(
        self,
        datafile: Dataset,
        preserve: str = "shape",
        pattern: str = BASE_CACHE_PATTERN,
        ext: str = ".shp",
        **kwargs,
    ) -> DownloadReturn:
        """
        Performs a download (through http, https of ftp protocol) to a tempfile
        which will be cleaned afterwards ; unzip targeted files to a temporary
        file which ** WILL ** need manual cleanup and sanitize geometries.
        In case of an actual download (success of download AND the file is a
        new one), the dict returned will be ot this form :
            {
                "downloaded": True,
                "hash": the archive's new hash value (before uncompression),
                "path": the temporary folder containing the targeted files
                }
        In case of failure (failure to download OR the file is the same as a
        previous one), the dict returned will be of this form ;
            {"downloaded": False, "hash": None, "path": None}

        Parameters
        ----------
        datafile : Dataset
            Dataset object to download.
        preserve : str, optional
            Specify which geometric option should be preserved while doing
            geometry sanitation (either shape or topology).
            The default is "shape".
        pattern : str, optional
            Pattern to validate. The default is BASE_CACHE_PATTERN.
        ext : str, optional
            File extension to look for (.shp, .gpkg, etc.). In order to
            preserve all auxiliary file (in case of shapefile for instance:
            .prj, .dbf, ...) all files sharing the same name (but it's
            extension) as one validated will be decompressed as well.
            The default is .shp.
        **kwargs :
            Optional arguments to pass to requests.Session object.

        Raises
        ------
        ValueError
            If preserve not amont 'shape' or 'topology'

        Returns
        -------
        DownloadReturn
            Dictionnaire doté du contenu suivant :
                downloaded: bool
                hash: str
                path: str
        """

        if preserve not in ["shape", "topology"]:
            msg = (
                "preserve must be either of 'shape', 'topology' - found "
                f"'{preserve}' instead"
            )
            raise ValueError(msg)

        hash = datafile.md5
        url = datafile.get_path_from_provider()

        if url.startswith(("http", "https")):
            func = self.download_to_tempfile_http
        else:
            params_ftp = datafile.sources["FTP"]
            self.connect(host=params_ftp["hostname"])
            self.login(user=params_ftp["username"], passwd=params_ftp["pwd"])
            func = self.download_to_tempfile_ftp

        # Download to temporary file
        downloaded, temp_archive_file_raw = func(url, hash, **kwargs)

        if not downloaded:
            # Suppression du fichier temporaire
            try:
                os.unlink(temp_archive_file_raw)
            except TypeError:
                pass
            return {"downloaded": False, "hash": None, "path": None}

        try:
            # Calcul du hashage du fichier brut (avant dézipage)
            hash = datafile.__md5__(temp_archive_file_raw)

            datafile.set_temp_file_path(temp_archive_file_raw)
            shp_location = datafile.unzip(pattern, preserve, ext=ext)
        except Exception as e:
            raise e
        finally:
            os.unlink(temp_archive_file_raw)

        return {"downloaded": True, "hash": hash, "path": shp_location}


def download_sources(
    providers: list,
    dataset_family: list,
    sources: list,
    territories: list,
    years: list,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
) -> dict:
    """
    Main function to perform downloads of datasets to store to the s3.
    All available combinations will be tested; hence an unfound file might not
    be an error given the fact that it might correspond to an unexpected
    combination; those be printed as warnings in the log.


    Parameters
    ----------
    providers : list
        List of providers in the yaml file
    dataset_family : list
        List of datasets family in the yaml file
    sources : list
        List of sources in the yaml file
    territories : list
        List of territoires in the yaml file
    years : list
        List of years in the yaml file

    Returns
    -------
    files : dict
        Structure of the nested dict will use the following keys :
            provider
                dataset_family
                    source
                        territory
                            year
                            {downloaded: bool, hash: str, path: str}
        For instance:
            {
                'IGN': {
                    'ADMINEXPRESS': {
                        'EXPRESS-COG-TERRITOIRE': {
                            'guadeloupe': {
                                2022: {
                                    'downloaded': True,
                                    'hash': '448ec7804a0671e13df7b39621f74dcd',
                                    'path': ('C:\\Users\\THOMAS~1.GRA\\AppData\\Local\\Temp\\tmppi3lxass\\ADMIN-EXPRESS-COG_3-1__SHP_RGAF09UTM20_GLP_2022-04-15\\ADMIN-EXPRESS-COG\\1_DONNEES_LIVRAISON_2022-04-15\\ADECOG_3-1_SHP_RGAF09UTM20_GLP',)
                                }, 2023: {
                                    'downloaded': False,
                                    'path': None,
                                    'hash': None
                                }
                            },
                            'martinique': {
                                2022: {
                                    'downloaded': True,
                                    'hash': '57a4c26167ed3436a0b2f53e11467e1b',
                                    'path': ('C:\\Users\\THOMAS~1.GRA\\AppData\\Local\\Temp\\tmpbmubfnsc\\ADMIN-EXPRESS-COG_3-1__SHP_RGAF09UTM20_MTQ_2022-04-15\\ADMIN-EXPRESS-COG\\1_DONNEES_LIVRAISON_2022-04-15\\ADECOG_3-1_SHP_RGAF09UTM20_MTQ',)
                                },
                                2023: {
                                    'downloaded': False,
                                    'path': None,
                                    'hash': None
                                }
                            }
                        }
                    }
                }
            }

    """
    combinations = list(
        product(sources, territories, years, providers, dataset_family)
    )

    files = {}
    with MasterScraper() as s:
        for source, territory, year, provider, dataset_family in combinations:
            logger.info(
                f"{provider} {dataset_family} {source} {territory} {year}"
            )

            datafile = Dataset(
                dataset_family,
                source,
                year,
                provider,
                territory,
                bucket,
                path_within_bucket,
            )

            # TODO : certains fichiers sont téléchargés plusieurs fois, par ex.
            # les EXPRESS-COG-TERRITOIRE d'avant 2020... -> à optimiser au cas
            # où ça se produirait avec des jeux de données plus récents ?

            # TODO : gérer les extensions dans le yaml ?
            try:
                result = s.download_unzip(
                    datafile,
                    preserve="shape",
                    pattern=BASE_CACHE_PATTERN,
                    ext=".shp",
                )
            except ValueError:
                logger.warning(f"{datafile} failed")

                this_result = {
                    provider: {
                        dataset_family: {
                            source: {
                                territory: {
                                    year: {
                                        "downloaded": False,
                                        "path": None,
                                        "hash": None,
                                    }
                                }
                            }
                        }
                    }
                }

            else:
                this_result = {
                    provider: {
                        dataset_family: {source: {territory: {year: result}}}
                    }
                }
            files = deep_dict_update(files, this_result)

    return files


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # providers = ["IGN"]
    # dataset_family = ["BDTOPO"]
    # sources = ["REMOVE"]
    # territories = ["france_entiere"]  # "guadeloupe", "martinique"]
    # years = [2017]

    providers = ["IGN"]
    dataset_family = ["ADMINEXPRESS"]
    sources = ["EXPRESS-COG-TERRITOIRE"]
    territories = ["guadeloupe", "martinique"]
    years = [2022, 2023]

    # results = download_sources(
    #     providers, dataset_family, sources, territories, years
    # )
    # print(results)
