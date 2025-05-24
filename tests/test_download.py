# -*- coding: utf-8 -*-

import pytest
import os
import requests_cache
import logging

from cartiflette.download.dataset import Dataset
from cartiflette.download.scraper import (
    MasterScraper,
    validate_file,
    download_to_tempfile_http,
)
from cartiflette.download import download_all
from cartiflette.utils import import_yaml_config
from tests.conftest import (
    DUMMY_FILE_1,
    DUMMY_FILE_2,
    HASH_DUMMY,
)


logger = logging.getLogger(__name__)


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


def test_file_validation():
    """
    test la validation des fichiers (méthode statique)
    """
    assert validate_file(DUMMY_FILE_1, HASH_DUMMY)
    assert not validate_file(DUMMY_FILE_2, HASH_DUMMY)


def test_http_proxy():
    """
    Test du bon fonctionnement du proxy

    """
    init = {}
    for k in ["http", "https"]:
        try:
            init[f"{k}_proxy"] = os.environ[f"{k}_proxy"]
        except KeyError:
            pass

    os.environ["http_proxy"] = "blah"
    try:
        del os.environ["https_proxy"]
    except KeyError:
        pass
    dummy_scraper = MasterScraper()
    try:
        assert isinstance(dummy_scraper, requests_cache.CachedSession)
        assert dummy_scraper.proxies == {
            "http": "blah",
        }
    except Exception as e:
        raise e
    finally:
        os.environ.update(init)


def test_http_download(mock_httpscraper_download_success):
    """
    test de download_to_tempfile_http
    """

    # Initialisation
    dummy_scraper = MasterScraper()
    dummy = "https://dummy"

    # Fourniture du même hash -> pas de téléchargement
    result = download_to_tempfile_http(
        url=dummy, hash_=HASH_DUMMY, session=dummy_scraper
    )
    downloaded, filetype, path = result
    assert not downloaded

    # Fourniture d'un hash changé -> téléchargement
    result = download_to_tempfile_http(
        url=dummy, hash_="BLAH", session=dummy_scraper
    )
    downloaded, filetype, path = result
    assert downloaded

    # Pas de hash fourni : valide le fichier a posteriori avec sa longueur
    result = download_to_tempfile_http(url=dummy, session=dummy_scraper)
    downloaded, filetype, path = result
    assert downloaded
    assert path


def test_download_ko_length(
    mock_httpscraper_download_success_corrupt_length,
):
    """
    test avec un mockup qui ne retourne aucun md5 dans le header et dont
    le Content-length ne correspond pas au contenu (simule un fichier corrompu)
    -> doit déclencher un IOError
    """
    dummy_scraper = MasterScraper()
    with pytest.raises(IOError):
        result = download_to_tempfile_http("dummy", session=dummy_scraper)


def test_download_ko_md5(
    mock_httpscraper_download_success_corrupt_hash,
):
    """
    test avec un mockup qui retourne un md5 dans le header incohérent
    avec son contenu (simule un fichier corrompu)
    -> doit déclencher un IOError
    """
    dummy_scraper = MasterScraper()
    with pytest.raises(IOError):
        result = download_to_tempfile_http("dummy", session=dummy_scraper)


# def test_MasterScraper_ko():
#     """
#     download_unzip
#     * Tester que si échec du téléchargement, le fichier temporaire est supprimé
#     * contrôler que le retour est un dictionnaire avec les bonnes clefs
#     """
#     # TODO
#     pass


# def test_MasterScraper_ok():
#     """
#     download_unzip
#     * Si succès, contrôle de la présence du fichier temporaire (puis le
#       supprimer)
#     * contrôler que le retour est un dictionnaire avec les bonnes clefs
#     """
#     # TODO
#     pass


def test_sources_yaml(mock_Dataset_without_s3):
    yaml = import_yaml_config()

    errors_type0 = []
    errors_type1 = []
    errors_type2 = []
    errors_type3 = []
    errors_type4 = []

    with MasterScraper() as scraper:
        for provider, provider_yaml in yaml.items():
            if not isinstance(provider_yaml, dict):
                continue

            for dataset_family, dataset_family_yaml in provider_yaml.items():
                if not isinstance(dataset_family_yaml, dict):
                    continue

                for source, source_yaml in dataset_family_yaml.items():
                    str_yaml = f"{dataset_family}/{source}"

                    if not isinstance(source_yaml, dict):
                        if source in {"pattern", "structure"}:
                            continue
                        else:
                            errors_type0.append(
                                f"yaml {str_yaml} contains '{source_yaml}'"
                            )
                            continue

                    years = set(source_yaml.keys()) - {"territory"}
                    try:
                        territories = set(source_yaml["territory"].keys())
                    except KeyError:
                        territories = {""}

                    print("-" * 50)
                    print(str_yaml, territories)
                    for year in years:
                        for territory in territories:
                            str_yaml = (
                                f"{dataset_family}/{source}/{year}/{provider}"
                                f"/{territory}"
                            )

                            if territory == "":
                                territory = None
                            try:
                                print(str_yaml)
                                ds = Dataset(
                                    dataset_family,
                                    source,
                                    int(year),
                                    provider,
                                    territory,
                                )
                            except Exception:
                                errors_type1.append(
                                    f"error on yaml {str_yaml} : dataset not "
                                    "constructed"
                                )
                                continue
                            try:
                                url = ds.get_path_from_provider()
                            except Exception:
                                errors_type2.append(
                                    f"error on yaml {str_yaml} : url not "
                                    "reconstructed"
                                )
                                continue

                            try:
                                # Use .head(url) instead of
                                # .get(url, stream=True) until bug of
                                # CachedSession is fixed
                                # See https://github.com/requests-cache/requests-cache/issues/878
                                r = scraper.head(url)
                            except Exception as e:
                                errors_type3.append(
                                    f"error on yaml {str_yaml} : "
                                    f"https head request failed on {url} "
                                    f"with exception {e}"
                                )
                                continue
                            if not r.ok:
                                errors_type4.append(
                                    f"error on yaml {str_yaml} : "
                                    "https head request "
                                    f"got code {r.status_code} on {url}"
                                )
    if errors_type0:
        logger.warning(
            "Champs du YAML non testés\n" + "\n".join(errors_type0)
        )

    if errors_type1 + errors_type2 + errors_type3 + errors_type4:
        if errors_type1:
            logger.error("=" * 50)
            logger.error(
                "Objet(s) Dataset(s) non instancié(s)\n"
                + "\n".join(errors_type1)
            )
            logger.error("-" * 50)

        if errors_type2:
            logger.error("=" * 50)
            logger.error(
                "URL non reconstituées:\n" + "\n".join(errors_type2)
            )
            logger.error("-" * 50)

        if errors_type3:
            logger.error("=" * 50)
            logger.error("Erreur HTTP:\n" + "\n".join(errors_type3))
            logger.error("-" * 50)

        if errors_type4:
            logger.error("=" * 50)
            logger.error(
                "Requête HTTP avec code d'erreur:\n"
                + "\n".join(errors_type4)
            )
            logger.error("-" * 50)

    assert (
        len(errors_type1 + errors_type2 + errors_type3 + errors_type4) == 0
    )


def test_download_all(total_mock_s3):
    ret = download_all()
    assert isinstance(ret, dict)
    assert len(ret) > 0
