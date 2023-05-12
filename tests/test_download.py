# -*- coding: utf-8 -*-
"""
Created on Thu May 11 19:54:04 2023

@author: thomas.grandjean
"""

import pytest
import os
import requests
from tests.mockups import *
from cartiflette.download import (
    Dataset,
    BaseScraper,
    HttpScraper,
    FtpScraper,
    MasterScraper,
    download_sources,
)
from tests.conftest import (
    DUMMY_FILE_1,
    DUMMY_FILE_2,
    HASH_DUMMY,
)
from tests.mockups import (
    mock_httpscraper_download_success,
    mock_httpscraper_download_success_corrupt_hash,
    mock_httpscraper_download_success_corrupt_length,
)


def test_Dataset():
    """
    __md5__
    __get_last_md5__
    update_json_md5
    get_path_from_provider
    __sanitize_file__
    set_temp_file_path
    unzip
    """
    # TODO
    pass


def test_BaseScraper():
    """
    test la validation des fichiers (méthode statique)
    """
    assert BaseScraper.__validate_file__(DUMMY_FILE_1, HASH_DUMMY)
    assert not BaseScraper.__validate_file__(DUMMY_FILE_2, HASH_DUMMY)


def test_HttpScraper_proxy():
    """
    Test du bon fonctionnement du proxy

    """
    init = {}
    for k in ["http", "https", "ftp"]:
        try:
            init[f"{k}_proxy"] = os.environ[f"{k}_proxy"]
        except KeyError:
            pass

    os.environ["http_proxy"] = "blah"
    os.environ["ftp_proxy"] = "blah"
    try:
        del os.environ["https_proxy"]
    except KeyError:
        pass
    dummy_scraper = HttpScraper()
    try:
        assert isinstance(dummy_scraper, requests.Session)
        assert dummy_scraper.proxies == {
            "http": "blah",
            "ftp": "blah",
        }
    except Exception as e:
        raise e
    finally:
        os.environ.update(init)


def test_HttpScraper_download(mock_httpscraper_download_success):
    """
    test de self.download_to_tempfile_http
    """

    # Initialisation
    dummy_scraper = HttpScraper()

    # Fourniture du même hash -> pas de téléchargement
    result = dummy_scraper.download_to_tempfile_http(
        url="dummy", hash=HASH_DUMMY
    )
    downloaded, path = result
    assert not downloaded

    # Fourniture d'un hash changé -> téléchargement
    result = dummy_scraper.download_to_tempfile_http(url="dummy", hash="BLAH")
    downloaded, path = result
    assert downloaded

    # Pas de hash fourni : valide le fichier a posteriori avec sa longueur
    result = dummy_scraper.download_to_tempfile_http(url="dummy")
    downloaded, path = result
    assert downloaded
    assert path


def test_HttpScraper_download_ko_length(
    mock_httpscraper_download_success_corrupt_length,
):
    """
    test avec un mockup qui ne retourne aucun md5 dans le header et dont
    le Content-length ne correspond pas au contenu (simule un fichier corrompu)
    -> doit déclencher un IOError
    """
    dummy_scraper = HttpScraper()
    with pytest.raises(IOError):
        result = dummy_scraper.download_to_tempfile_http("dummy")


def test_HttpScraper_download_ko_md5(
    mock_httpscraper_download_success_corrupt_hash,
):
    """
    test avec un mockup qui retourne un md5 dans le header incohérent
    avec son contenu (simule un fichier corrompu)
    -> doit déclencher un IOError
    """
    dummy_scraper = HttpScraper()
    with pytest.raises(IOError):
        result = dummy_scraper.download_to_tempfile_http("dummy")


def test_FtpScraper():
    """
    download_to_tempfile_ftp
    """
    # TODO
    pass


def test_MasterScraper_ko():
    """
    download_unzip
    * Tester que si échec du téléchargement, le fichier temporaire est supprimé
    * contrôler que le retour est un dictionnaire avec les bonnes clefs
    """
    # TODO
    pass


def test_MasterScraper_ok():
    """
    download_unzip
    * Si succès, contrôle de la présence du fichier temporaire (puis le
      supprimer)
    * contrôler que le retour est un dictionnaire avec les bonnes clefs
    """
    # TODO
    pass


def test_download_sources():
    pass


def test_sources_yaml():
    pass
