# -*- coding: utf-8 -*-
"""
Created on Tue Mar 21 20:52:51 2023

@author: thomas.grandjean
"""

import requests
from tqdm import tqdm
import hashlib
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
from typing import Dict, Any, TypedDict
from glob import glob
import tempfile

from cartiflette.utils import import_yaml_config

BUCKET = "projet-cartiflette"
PATH_WITHIN_BUCKET = "diffusion/shapefiles-test1"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

BASE_CACHE_PATTERN = os.path.join("**", "*DONNEES_LIVRAISON*", "**")

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})


logger = logging.getLogger(__name__)

# =============================================================================
# A placer dans utils :
# =============================================================================


def hash_file(file_path):
    """
    https://gist.github.com/mjohnsullivan/9322154
    Get the MD5 hsah value of a file
    :param file_path: path to the file for hash validation
    :type file_path:  string
    :param hash:      expected hash value of the file
    """
    m = hashlib.md5()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(1000 * 1000)  # 1MB
            if not chunk:
                break
            m.update(chunk)
    return m.hexdigest()


def deep_update(
    mapping: Dict[Any, Any], *updating_mappings: Dict[Any, Any]
) -> Dict[Any, Any]:
    """
    https://stackoverflow.com/questions/3232943/#answer-68557484
    Recursive update of a nested dictionary

    Parameters
    ----------
    mapping : Dict[KeyType, Any]
        initial dictionary
    *updating_mappings : Dict[KeyType, Any]
        update to set into mapping

    Returns
    -------
    Dict[KeyType, Any]
        new (udpated) dictionary

    """

    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if (
                k in updated_mapping
                and isinstance(updated_mapping[k], dict)
                and isinstance(v, dict)
            ):
                updated_mapping[k] = deep_update(updated_mapping[k], v)
            else:
                updated_mapping[k] = v
    return updated_mapping


# =============================================================================


class Dataset:
    JSON_MD5 = f"{BUCKET}/{PATH_WITHIN_BUCKET}/md5.json"
    md5 = None

    def __init__(
        self,
        dataset_family: str = "ADMINEXPRESS",
        source: str = "EXPRESS-COG-TERRITOIRE",
        year: int = date.today().year,
        provider: str = "IGN",
        territory: str = None,
    ):
        self.dataset_family = dataset_family
        self.source = source
        self.year = year
        self.territory = territory
        self.provider = provider
        self.config_open_data = import_yaml_config()

        self.sources = self.config_open_data[provider][dataset_family][source]

        self.__get_last_md5__()

    @staticmethod
    def __md5__(file_path: str) -> str:
        return hash_file(file_path)

    def __get_last_md5__(self):
        "Récupération du dernier md5 sur le s3"

        # {provider} {dataset_family} {source} {territory} {year}

        try:
            with fs.open(self.JSON_MD5, "r") as f:
                all_md5 = json.load(f)
        except Exception as e:
            logger.warning(e)
            logger.warning("md5 not found")
            return

        md5 = all_md5[self.provider][self.dataset_family][self.source][
            self.territory
        ][self.year]
        self.md5 = md5

    def update_json_md5(self, md5: str) -> bool:
        "Mise à jour du json des md5"
        # TODO : à basculer dans utils
        md5 = {
            self.provider: {
                self.dataset_family: {
                    self.source: {self.territory: {self.year: md5}}
                }
            }
        }
        try:
            with fs.open(self.JSON_MD5, "r+") as f:
                all_md5 = json.load(f)
                all_md5 = deep_update(all_md5, md5)
                fs.write(json.dump(all_md5, f))
            return True

        except Exception as e:
            logger.warning(e)
            logger.warning("md5 not written")
            return False

    def get_path_from_provider(self) -> str:
        "Récupération du chemin de téléchargement d'après le yaml"

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

        if self.source.endswith("-TERRITOIRE"):
            field = sources["field"][self.territory]
            date = sources[year]["date"]
            prefix = sources[year]["prefix"]
            version = sources[year]["version"]
            structure = sources[year]["structure"]
            url_prefix = sources[year]["url_prefix"]

            url = structure.format(
                url_prefix=url_prefix,
                date=date,
                prefix=prefix,
                version=version,
                field=field,
            )

        else:
            url = sources[year]["file"]

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
        self.temp_archive_path = path

    def unzip(
        self, pattern: str = BASE_CACHE_PATTERN, preserve: str = "shape"
    ) -> str:
        # unzip in temp directory
        tmp = tempfile.TemporaryDirectory()
        location = tmp.name
        logger.debug(f"Extracting to {location}")

        # TODO : py7zr semble très lent !
        with py7zr.SevenZipFile(self.temp_archive_path, mode="r") as archive:
            files = archive.getnames()
            shapefiles = {
                os.path.splitext(x)[0]
                for x in files
                if x.lower().endswith(".shp")
            }
            logger.debug(shapefiles)
            targets = [
                x for x in files if os.path.splitext(x)[0] in shapefiles
            ]
            logger.debug(targets)
            logger.info(len(targets))
            archive.extract(path=location, targets=targets)

        year = self.year
        source = self.source
        territory = self.territory
        sources = self.sources

        # note that glob is case sensitive on linux
        combinations_patterns = [[pattern.lower(), pattern.upper()]]

        if year <= 2020 and source.endswith("-TERRITOIRE"):
            field_code = sources["field"][territory].split("_")[0]
            pattern = os.path.join(f"*{field_code}*", "**")
            combinations_patterns.append([pattern.lower(), pattern.upper()])

        combinations_patterns.append(["*.shp", "*.SHP"])

        patterns = list(product(*combinations_patterns))
        paths = []
        for list_pattern in patterns:
            pattern = os.path.join(*list_pattern)
            paths += glob(os.path.join(location, pattern), recursive=True)

        for file in paths:
            self.__sanitize_file__(file, preserve=preserve)

        shp_location = os.path.dirname(paths[0])
        return shp_location


class BaseScraper:
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for protocol in ["http", "https", "ftp"]:
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

        # TODO : faire une recherche dans le header pour confirmer qu'on
        # est bien sur un 7z

        with tempfile.NamedTemporaryFile("wb", delete=False) as temp_file:
            file_path = temp_file.name
            logger.debug(f"Downloading to {file_path}")

            logger.debug(f"starting download at {url}")
            r = super().get(url, stream=True, **kwargs)
            with tqdm(
                desc="Downloading: ",
                total=expected_file_size / block_size,
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
            os.unlink(file_path)
            return False, None

        return True, file_path


class FtpScraper(BaseScraper, ftplib.FTP):
    def download_to_tempfile_ftp(
        self, url: str, hash: str = None, **kwargs
    ) -> tuple[bool, str]:
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
    class DownloadReturn(TypedDict):
        downloaded: bool
        hash: str
        path: str

    def download_unzip(
        self,
        datafile: Dataset,
        preserve: str = "shape",
        pattern: str = BASE_CACHE_PATTERN,
        **kwargs,
    ) -> DownloadReturn:
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
            os.unlink(temp_archive_file_raw)
            return {"downloaded": False, "hash": None, "path": None}

        try:
            # Calcul du hashage du fichier brut (avant dézipage)
            hash = datafile.__md5__(temp_archive_file_raw)

            datafile.set_temp_file_path(temp_archive_file_raw)
            shp_location = datafile.unzip(pattern, preserve)
        except Exception as e:
            raise e
        finally:
            os.unlink(temp_archive_file_raw)

        return {"downloaded": True, "hash": hash, "path": shp_location}


def download_sources():
    providers = ["IGN"]
    dataset_family = ["ADMINEXPRESS"]
    sources = ["EXPRESS-COG-TERRITOIRE"]
    territories = ["guadeloupe", "martinique"]
    years = [2021]

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
                dataset_family, source, year, provider, territory
            )

            result = s.download_unzip(
                datafile, preserve="shape", pattern=BASE_CACHE_PATTERN
            )

            this_result = {
                provider: {
                    dataset_family: {source: {territory: {year: result}}}
                }
            }
            files = deep_update(files, this_result)

    return files


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(download_sources())