# -*- coding: utf-8 -*-

import magic
import ftplib
from glob import glob
import logging
import numpy as np
import os
import re
import requests_cache
import tempfile
from tqdm import tqdm
from typing import TypedDict
from unidecode import unidecode
from urllib3.util import parse_url

from cartiflette.utils import hash_file
from cartiflette.download.dataset import Dataset
from cartiflette.download.layer import Layer
from cartiflette import LEAVE_TQDM

logger = logging.getLogger(__name__)


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

    pattern_path = re.compile(r"[\\/]")

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

        backend = requests_cache.SQLiteCache(
            db_path=cache_name, wal=True, check_same_thread=False
        )

        super().__init__(
            backend=backend,
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
            logger.debug(f"md5 not found in header at url {url}")
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
                logger.debug(msg)

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
                leave=LEAVE_TQDM,
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
            logger.debug(f"md5 matched at {url} after download")
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
                    "refering https://stackoverflow.com/questions/45472577/"
                    "#answer-58282569"
                )
            if parsed.scheme == "ftp":
                raise NotImplementedError(
                    "Connection with protocol FTP through FTP corporate "
                    "proxy is not implemented yet; please create an issue "
                    "refering https://stackoverflow.com/questions/45472577/"
                    "#answer-51879035"
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
                leave=LEAVE_TQDM,
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
        layers: dict
        root_cleanup: str

    def download_unpack(self, datafile: Dataset, **kwargs) -> DownloadReturn:
        """
        Performs a download (through http, https of ftp protocol) to a tempfile
        which will be cleaned automatically ; unzip targeted files to a 2nd
        temporary file which ** WILL ** need manual cleanup.
        In case of an actual download (success of download AND the file is a
        new one), the dict returned will be ot this form :
            {
                "downloaded": True,
                "hash": the archive's new hash value (before uncompression),
                "layers": dict of Layer objects
                "root_cleanup": the root temporary directory to cleanup
                    afterwards
                }
        In case of failure (failure to download OR the file is the same as a
        previous one), the dict returned will be of this form ;
            {"downloaded": False, "hash": None, "layers": None}

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
                layers: ...
                root_cleanup: str
                    root temporary directory to cleanup afterwards

        Ex:
            {
                'downloaded': True,
                'hash': '5435fca3e488ca0372505b9bcacfde30',
                'layers': {
                    'CONTOURS-IRIS_2-1_SHP_RGR92UTM40S_REU-2022_CONTOURS-IRIS':
                        < Layer CONTOURS - IRIS_2 - 1_SHP_RGR92UTM40S_REU - 2022_CONTOURS - IRIS from < Dataset IGN CONTOUR - IRIS ROOT None 2022 >> ,
                    'CONTOURS-IRIS_2-1_SHP_RGAF09UTM20_GLP-2022_CONTOURS-IRIS':
                        < Layer CONTOURS - IRIS_2 - 1_SHP_RGAF09UTM20_GLP - 2022_CONTOURS - IRIS from < Dataset IGN CONTOUR - IRIS ROOT None 2022 >> ,
                    'CONTOURS-IRIS_2-1_SHP_RGAF09UTM20_MTQ-2022_CONTOURS-IRIS':
                        < Layer CONTOURS - IRIS_2 - 1_SHP_RGAF09UTM20_MTQ - 2022_CONTOURS - IRIS from < Dataset IGN CONTOUR - IRIS ROOT None 2022 >> ,
                    'CONTOURS-IRIS_2-1_SHP_LAMB93_FXX-2022_CONTOURS-IRIS':
                        < Layer CONTOURS - IRIS_2 - 1_SHP_LAMB93_FXX - 2022_CONTOURS - IRIS from < Dataset IGN CONTOUR - IRIS ROOT None 2022 >> ,
                    'CONTOURS-IRIS_2-1_SHP_UTM22RGFG95_GUF-2022_CONTOURS-IRIS':
                        < Layer CONTOURS - IRIS_2 - 1_SHP_UTM22RGFG95_GUF - 2022_CONTOURS - IRIS from < Dataset IGN CONTOUR - IRIS ROOT None 2022 >> ,
                    'CONTOURS-IRIS_2-1_SHP_RGM04UTM38S_MYT-2022_CONTOURS-IRIS':
                        < Layer CONTOURS - IRIS_2 - 1_SHP_RGM04UTM38S_MYT - 2022_CONTOURS - IRIS from < Dataset IGN CONTOUR - IRIS ROOT None 2022 >>
                },
                'root_cleanup': 'C:\\Users\\tintin.milou\\AppData\\Local\\Temp\\tmpnbvoes9g'
            }


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
                "layers": None,
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
            elif "Unicode text" in filetype or "CSV text" in filetype:
                # copy in temp directory without processing
                root_folder = tempfile.mkdtemp()
                with open(temp_archive_file_raw, "rb") as f:
                    filename = unidecode(datafile.__str__().upper()).strip()
                    filename = "_".join(x for x in re.split(r"\W+", filename) if x)
                    path = os.path.join(root_folder, filename + ".csv")
                    with open(path, "wb") as out:
                        out.write(f.read())

                logger.debug(f"Storing CSV to {root_folder}")
                files_locations = ((path,),)

            else:
                raise NotImplementedError(f"{filetype} encountered")
        except Exception as e:
            raise e
        finally:
            os.unlink(temp_archive_file_raw)

        # Find discriminant names for files
        basenames = {}
        k = 1
        spliter = re.compile(r"[\\/\.]")
        while len(basenames) != len(files_locations):
            k += 1
            basenames = {
                os.sep.join(spliter.split(file)[-k:-1]): cluster
                for cluster in files_locations
                for file in cluster
            }
            if k > 10:
                raise Exception("Stop this")

        paths = {
            spliter.sub("_", basename): cluster
            for basename, cluster in basenames.items()
        }

        layers = dict()
        for cluster_name, cluster_filtered in paths.items():
            cluster_pattern = {os.path.splitext(x)[0] for x in cluster_filtered}.pop()
            all_files_cluster = glob(os.path.join(cluster_pattern + ".*"))
            all_files_cluster = [
                self.pattern_path.sub("/", x) for x in all_files_cluster
            ]
            cluster_filtered = {self.pattern_path.sub("/", x) for x in cluster_filtered}

            dict_files = {x: (x in cluster_filtered) for x in all_files_cluster}

            layers[cluster_name] = Layer(datafile, cluster_name, dict_files)

        return {
            "downloaded": True,
            "hash": hash,
            "layers": layers,
            "root_cleanup": root_folder,
        }
