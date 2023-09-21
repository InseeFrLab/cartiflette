# -*- coding: utf-8 -*-

from datetime import date
from itertools import product
import logging
import s3fs
import shutil

import cartiflette
from cartiflette.utils import deep_dict_update
from cartiflette.download.scraper import MasterScraper
from cartiflette.download.dataset import Dataset

logger = logging.getLogger(__name__)

# TODO :
#    * check all docstrings
#    * aller récupérer la fonction du dernier push de Lino pour aller
#      chercher l'URL MinIO sur /utils/
#    * réfléchir au cas des IRIS qui mériteraient d'être renommés avec leur niveau géo


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

    results = []

    # providers = ["IGN"]
    # dataset_family = ["ADMINEXPRESS"]
    # sources = ["EXPRESS-COG-TERRITOIRE"]
    # territories = ["guadeloupe", "martinique"]
    # years = [2022, 2023]
    # results.append(
    #     download_sources(
    #         providers, dataset_family, sources, territories, years
    #     )
    # )

    # providers = ["IGN"]
    # dataset_family = ["BDTOPO"]
    # sources = ["ROOT"]
    # territories = ["france_entiere"]
    # years = [2017]
    # results.append(
    #     download_sources(
    #         providers, dataset_family, sources, territories, years
    #     )
    # )

    # providers = ["IGN"]
    # dataset_family = ["CONTOUR-IRIS"]
    # sources = ["ROOT"]
    # territories = ["france_entiere"]
    # years = [2022]
    # results.append(
    #     download_sources(
    #         providers, dataset_family, sources, territories, years
    #     )
    # )

    # providers = ["Insee"]
    # dataset_family = ["COG"]
    # sources = ["COMMUNE"]
    # territories = (None,)
    # years = [
    #     2022,
    #     2021,
    #     2018,
    # ]
    # results.append(
    #     download_sources(
    #         providers, dataset_family, sources, territories, years
    #     )
    # )

    providers = ["Insee"]
    dataset_family = ["BV"]
    sources = ["FondsDeCarte_BV_2022"]
    territories = (None,)
    years = [2023, 2022]
    results.append(
        download_sources(
            providers, dataset_family, sources, territories, years
        )
    )

    providers = ["Insee"]
    dataset_family = ["BV"]
    sources = ["FondsDeCarte_BV_2012"]
    territories = (None,)
    years = [2022]
    results.append(
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

#                     years = set(source_yaml.keys()) - {"territory", "FTP"}
#                     try:
#                         territories = set(source_yaml["territory"].keys())
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
