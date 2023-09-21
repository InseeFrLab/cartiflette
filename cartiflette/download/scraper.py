# -*- coding: utf-8 -*-

import magic
from datetime import date
import ftplib
from itertools import product
import logging
import numpy as np
import os
import re
import requests_cache
import s3fs
import shutil
import tempfile
from tqdm import tqdm
from typing import TypedDict
from unidecode import unidecode
from urllib3.util import parse_url

import cartiflette
from cartiflette.utils import hash_file, deep_dict_update
from .dataset import Dataset


logger = logging.getLogger(__name__)

# TODO : check all docstrings


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


class HttpScraper(
    BaseScraper,
    # requests.Session,
    requests_cache.CachedSession,
):
    """
    Scraper with specific download method for http/https get protocol. Not
    meant to be used by itself, but only when surcharged.
    """

    def __init__(
        self,
        cache_name: str = "cartiflette.sqlite",
        expire_after: int = 3600 * 24 * 2,  # 2days cache
        *args,
        **kwargs,
    ):
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
        super().__init__(
            cache_name,
            expire_after=expire_after,
            *args,
            **kwargs,
        )
        # requests_cache.install_cache(cache_name, expire_after=expire_after)

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
        tuple[bool, str, str]
            bool : True if a new file has been downloaded, False in other cases
            str : File type (as returned by web requests, None if fails)
            str : path to the temporary file if bool was True (else None)
        """

        # ignore kwargs["stream"] if it is passed in kwargs
        try:
            del kwargs["stream"]
        except KeyError:
            pass

        block_size = 1024 * 1024  # 1MiB

        # check file's characteristics
        r = super().head(url, stream=True, **kwargs)
        head = r.headers

        if not r.ok:
            raise IOError(f"download failed with {r.status_code} code")

        try:
            expected_md5 = head["content-md5"]

            logger.debug(f"File MD5 is {expected_md5}")
        except KeyError:
            expected_md5 = None
            logger.info(f"md5 not found in header at url {url}")
        else:
            if hash and expected_md5 == hash:
                # unchanged file -> exit
                logger.info(f"md5 matched at {url} - download prevented")
                return False, None, None
        finally:
            try:
                # No MD5 in header -> check requested file's size
                expected_file_size = int(head["Content-length"])
                logger.debug(f"File size is {expected_file_size}")
            except KeyError:
                expected_file_size = None
                msg = f"Content-Length not found in header at url {url}"
                logger.info(msg)

        with tempfile.NamedTemporaryFile("wb", delete=False) as temp_file:
            file_path = temp_file.name
            logger.debug(f"Downloading to {file_path}")

            logger.debug(f"starting download at {url}")
            r = self.get(url, stream=True, **kwargs)
            if not r.ok:
                raise IOError(f"download failed with {r.status_code} code")

            if expected_file_size:
                total = int(np.ceil(expected_file_size / block_size))
            else:
                total = None
            with tqdm(
                desc="Downloading: ",
                total=total,
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
                os.unlink(file_path)
                raise IOError("download failed (corrupted file)")
        elif expected_file_size:
            # check that the downloaded file is the expected size
            if not expected_file_size == os.path.getsize(file_path):
                os.unlink(file_path)
                raise IOError("download failed (corrupted file)")

        # if there's a hash value, check if there are any changes
        if hash and self.__validate_file__(file_path, hash):
            # unchanged file -> exit (after deleting the downloaded file)
            logger.info(f"md5 matched at {url} after download")
            os.unlink(file_path)
            return False, None, None

        filetype = magic.from_file(file_path)

        return True, filetype, file_path


class FtpScraper(BaseScraper, ftplib.FTP):
    """
    Scraper with specific download method for ftp protocol. Not meant to be
    used by itself, but only when surcharged.
    """

    def __init__(self, *args, **kwargs):
        try:
            proxy = os.environ["ftp_proxy"]
        except KeyError:
            super().__init__(*args, **kwargs)
        else:
            parsed = parse_url(proxy)
            if parsed.scheme == "http":
                raise NotImplementedError(
                    "Connection with protocol FTP through HTTP corporate "
                    "proxy is not implemented yet; please create an issue "
                    "refering https://stackoverflow.com/questions/45472577/#answer-58282569"
                )
            if parsed.scheme == "ftp":
                raise NotImplementedError(
                    "Connection with protocol FTP through FTP corporate "
                    "proxy is not implemented yet; please create an issue "
                    "refering https://stackoverflow.com/questions/45472577/#answer-51879035"
                )

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
        tuple[bool, str, str]
            bool : True if a new file has been downloaded, False in other cases
            str : file type
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

                def download_write(data):
                    pbar.update(len(data))
                    file.write(data)

                self.retrbinary(f"RETR {url}", download_write)

        # check that the downloaded file is the expected size
        if not expected_file_size == os.path.getsize(file_path):
            raise IOError("download failed (corrupted file)")

        # if there's a hash value, check if there are any changes
        if hash and self.__validate_file__(file_path, hash):
            # unchanged file -> exit (after deleting the downloaded file)
            os.unlink(file_path)
            return False, None, None

        filetype = magic.from_file(file_path)

        return True, filetype, file_path


class MasterScraper(HttpScraper, FtpScraper):
    """
    Scraper main class which could be used to perform either http/https get
    downloads of ftp downloads.
    """

    class DownloadReturn(TypedDict):
        downloaded: bool
        hash: str
        paths: tuple
        root_cleanup: str

    def download_unpack(self, datafile: Dataset, **kwargs) -> DownloadReturn:
        """
        Performs a download (through http, https of ftp protocol) to a tempfile
        which will be cleaned afterwards ; unzip targeted files to a temporary
        file which ** WILL ** need manual cleanup.
        In case of an actual download (success of download AND the file is a
        new one), the dict returned will be ot this form :
            {
                "downloaded": True,
                "hash": the archive's new hash value (before uncompression),
                "paths": the targeted (and auxiliary) files extracted
                "root_cleanup": the root temporary directory to cleanup later
                }
        In case of failure (failure to download OR the file is the same as a
        previous one), the dict returned will be of this form ;
            {"downloaded": False, "hash": None, "paths": None}

        Parameters
        ----------
        datafile : Dataset
            Dataset object to download.
        **kwargs :
            Optional arguments to pass to requests.Session object.

        Returns
        -------
        DownloadReturn
            Dictionnaire doté du contenu suivant :
                downloaded: bool
                hash: str
                paths: tuple (of str)
                    paths to all files matching the pattern having been
                    extracted (should be only one path except with shapefiles)
                root_cleanup: str
                    root temporary directory to cleanup afterwards

        """

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
        downloaded, filetype, temp_archive_file_raw = func(url, hash, **kwargs)

        if not downloaded:
            # Suppression du fichier temporaire
            try:
                os.unlink(temp_archive_file_raw)
            except TypeError:
                pass
            return {
                "downloaded": False,
                "hash": None,
                "paths": None,
                "root_cleanup": None,
            }

        try:
            # Calcul du hashage du fichier brut (avant dézipage)
            hash = datafile._md5(temp_archive_file_raw)

            datafile.set_temp_file_path(temp_archive_file_raw)

            if "7-zip" in filetype:
                root_folder, files_locations = datafile.unpack(protocol="7z")
            elif "Zip archive" in filetype:
                root_folder, files_locations = datafile.unpack(protocol="zip")
            elif "Unicode text" in filetype:
                # copy in temp directory without processing
                root_folder = tempfile.mkdtemp()
                with open(temp_archive_file_raw, "rb") as f:
                    filename = unidecode(datafile.__str__().upper()).strip()
                    filename = "_".join(
                        x for x in re.split(r"\W+", filename) if x
                    )
                    path = os.path.join(root_folder, filename + ".csv")
                    with open(path, "wb") as out:
                        out.write(f.read())
                logger.debug(f"Storing CSV to {root_folder}")
                files_locations = (path,)

            else:
                raise NotImplementedError(f"{filetype} encountered")
        except Exception as e:
            raise e
        finally:
            os.unlink(temp_archive_file_raw)

        return {
            "downloaded": True,
            "hash": hash,
            "paths": files_locations,
            "root_cleanup": root_folder,
        }


def upload_vectorfile_to_s3(
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-TERRITOIRE",
    year: int = None,
    provider: str = "IGN",
    territory: str = "guyane",
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = cartiflette.FS,
    base_cache_pattern: str = cartiflette.BASE_CACHE_PATTERN,
):
    """
    Download exactly one dataset from it's provider's website and upload it to
    cartiflette S3 storage. If the file is already there and uptodate, the file
    will not be overwritten on S3.

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
        Territory described in the yaml file. The default is "guyane".
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.
    base_cache_pattern : str, optional
        Pattern to validate. The default is cartiflette.BASE_CACHE_PATTERN.

    Returns
    -------
    None.

    """

    if not year:
        year = date.today().year

    with MasterScraper() as s:
        datafile = Dataset(
            dataset_family,
            source,
            year,
            provider,
            territory,
            bucket,
            path_within_bucket,
            fs,
        )

        result = s.download_unpack(
            datafile,
            pattern=base_cache_pattern,
        )

        normalized_path_bucket = (
            f"{year=}/raw/{provider=}/{source=}/{territory=}"
        )
        normalized_path_bucket = normalized_path_bucket.replace("'", "")

        if not result["downloaded"]:
            logger.info("File already there and uptodate")
            return

        # DUPLICATE SOURCES IN BUCKET
        errors_encountered = False
        for path_local in result["path"]:
            try:
                logger.info(f"Iterating over {path_local}")

                fs.put(
                    path_local,
                    f"{bucket}/{path_within_bucket}/{normalized_path_bucket}",
                    recursive=True,
                )
            except Exception as e:
                logger.error(e)
                errors_encountered = True
            finally:
                # cleanup temp files
                shutil.rmtree(path_local)

        if not errors_encountered:
            # NOW WRITE MD5 IN BUCKET ROOT (in case of error, should be skipped
            # to allow for further tentatives)
            datafile.update_json_md5(result["hash"])


def download_sources(
    providers: list,
    dataset_family: list,
    sources: list,
    territories: list,
    years: list,
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
) -> dict:
    # TODO : attention à MAJ la sortie (contenu du dict avec chemins complets et
    # root folder)
    """
    Main function to perform downloads of datasets to store to the s3.
    All available combinations will be tested; hence an unfound file might not
    be an error given the fact that it might correspond to an unexpected
    combination; those will be printed as warnings in the log.


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
                                    'paths': ('C:\\Users\\THOMAS~1.GRA\\AppData\\Local\\Temp\\tmpbmubfnsc\\ADMIN-EXPRESS-COG_3-1__SHP_RGAF09UTM20_MTQ_2022-04-15\\ADMIN-EXPRESS-COG\\1_DONNEES_LIVRAISON_2022-04-15\\ADECOG_3-1_SHP_RGAF09UTM20_MTQ',)
                                },
                                2023: {
                                    'downloaded': False,
                                    'paths': None,
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
                f"Download {provider} {dataset_family} {source} {territory} {year}"
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
                result = s.download_unpack(datafile)
            except ValueError as e:
                logger.error(e)
                # logger.error(f"{datafile} 7-zip extraction failed")

                this_result = {
                    provider: {
                        dataset_family: {
                            source: {
                                territory: {
                                    year: {
                                        "downloaded": False,
                                        "paths": None,
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
                logger.info("Success")
            files = deep_dict_update(files, this_result)

    return files


def download_all():
    # Option 1 pour dérouler pipeline par bloc familial

    results = {}

    # providers = ["IGN"]
    # dataset_family = ["ADMINEXPRESS"]
    # sources = ["EXPRESS-COG-TERRITOIRE"]
    # territories = ["guadeloupe", "martinique"]
    # years = [2022, 2023]
    # results.update(
    #     download_sources(
    #         providers, dataset_family, sources, territories, years
    #     )
    # )

    # providers = ["IGN"]
    # dataset_family = ["BDTOPO"]
    # sources = ["REMOVE"]
    # territories = ["france_entiere"]
    # years = [2017]
    # results.update(
    #     download_sources(
    #         providers, dataset_family, sources, territories, years
    #     )
    # )

    providers = ["Insee"]
    dataset_family = ["COG"]
    sources = ["COMMUNE"]
    territories = (None,)
    years = [
        2022,
        2021,
        2018,
    ]
    results.update(
        download_sources(
            providers, dataset_family, sources, territories, years
        )
    )

    return results


# def download_all_option2():
#     # Dérouler le yaml comme dans le test

#     yaml = import_yaml_config()

#     with MasterScraper() as scraper:
#         for provider, provider_yaml in yaml.items():
#             if not isinstance(provider_yaml, dict):
#                 continue

#             for dataset_family, dataset_family_yaml in provider_yaml.items():
#                 if not isinstance(dataset_family_yaml, dict):
#                     continue

#                 for source, source_yaml in dataset_family_yaml.items():
#                     str_yaml = f"{dataset_family}/{source}"

#                     if not isinstance(source_yaml, dict):
#                         logger.error(
#                             f"yaml {str_yaml} contains '{source_yaml}'"
#                         )
#                         continue
#                     elif "FTP" in set(source_yaml.keys()):
#                         logger.info("yaml {str_yaml} not checked (FTP)")
#                         continue

#                     years = set(source_yaml.keys()) - {"field", "FTP"}
#                     try:
#                         territories = set(source_yaml["field"].keys())
#                     except KeyError:
#                         territories = {""}

#                     for year in years:
#                         for territory in territories:
#                             str_yaml = (
#                                 f"{dataset_family}/{source}/{year}/"
#                                 f"{provider}/{territory}"
#                             )

#                             if territory == "":
#                                 territory = None
#                             try:
#                                 ds = Dataset(
#                                     dataset_family,
#                                     source,
#                                     int(year),
#                                     provider,
#                                     territory,
#                                 )
#                             except Exception:
#                                 logger.error(
#                                     f"error on yaml {str_yaml} : "
#                                     "dataset not constructed"
#                                 )
#                                 continue
#                             try:
#                                 url = ds.get_path_from_provider()
#                             except Exception:
#                                 logger.error(
#                                     f"error on yaml {str_yaml} : "
#                                     "url no reconstructed"
#                                 )
#                                 continue

#                             try:
#                                 r = scraper.get(url, stream=True)
#                             except Exception:
#                                 logger.error(
#                                     f"error on yaml {str_yaml} : "
#                                     f"https get request failed on {url}"
#                                 )
#                                 continue
#                             if not r.ok:
#                                 logger.error(
#                                     f"error on yaml {str_yaml} : "
#                                     "https get request "
#                                     f"got code {r.status_code} on {url}"
#                                 )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # providers = ["IGN"]
    # dataset_family = ["BDTOPO"]
    # sources = ["REMOVE"]
    # territories = ["france_entiere"]  # "guadeloupe", "martinique"]
    # years = [2017]

    # =============================================================================
    #     providers = ["IGN"]
    #     dataset_family = ["ADMINEXPRESS"]
    #     sources = ["EXPRESS-COG-TERRITOIRE"]
    #     territories = ["guadeloupe", "martinique"]
    #     years = [2022, 2023]
    #
    #     # test = upload_vectorfile_to_s3(year=2022)
    #     # print(test)
    #
    #     results = download_sources(
    #         providers, dataset_family, sources, territories, years
    #     )
    #     print(results)
    # =============================================================================

    # test = upload_vectorfile_to_s3(
    #     dataset_family="COG",
    #     source="root",
    #     year=2022,
    #     provider="Insee",
    #     territory=None,
    #     bucket=cartiflette.BUCKET,
    #     path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    #     fs=cartiflette.FS,
    #     base_cache_pattern=cartiflette.BASE_CACHE_PATTERN,
    # )

    results = download_all()
