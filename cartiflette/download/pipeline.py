# -*- coding: utf-8 -*-

from datetime import date
import json
import logging
from pebble import ThreadPool
import s3fs

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS, THREADS_DOWNLOAD
from cartiflette.constants import DOWNLOAD_PIPELINE_ARGS
from cartiflette.download.download import _download_sources
from cartiflette.utils import deep_dict_update

logger = logging.getLogger(__name__)


def download_all(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
    upload: bool = True,
) -> dict:
    """
    Performs a full pipeline to download data and store them on MinIO. The
    target files are described in cartiflette/constants.py under the
    constant DOWNLOAD_PIPELINE_ARGS. Those files' characteristics must also be
    described in the cartiflette/utils/sources.yaml file.

    Note: to perform an easy debugging task, please overwrite
    cartiflette.config.THREADS_DOWNLOAD to 1 (to avoid multithreading which
    could be gruesome to debug).

    Parameters
    ----------
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.
    upload : bool, optional
        Whether to store data on MinIO or not. This argument should only be
        used for debugging purposes. The default is True, to upload data on
        MinIO.

    Returns
    -------
    results : dict
        Nest dictionnary describing the results and the path to each uploaded
        file. Each sub-result will indicate if an upload was performed or not
        under the key "downloaded". Note that if the uploaded files are a
        shapefile and it's auxiliary files, only the main (.shp) file will be
        stored into the results.

        Example of result:

        {
            'IGN': {
                'ADMINEXPRESS': {
                    'EXPRESS-COG-TERRITOIRE': {
                        'guadeloupe': {
                            2022: {
                                'downloaded': True,
                                'paths': {
                                    'CHFLIEU_COMMUNE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/CHFLIEU_COMMUNE.shp'],
                                    'REGION': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/REGION.shp'],
                                    'COMMUNE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/COMMUNE.shp'],
                                    'DEPARTEMENT': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/DEPARTEMENT.shp'],
                                    'EPCI': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/EPCI.shp'],
                                    'ARRONDISSEMENT': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/ARRONDISSEMENT.shp'],
                                    'CANTON': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/CANTON.shp'],
                                    'COLLECTIVITE_TERRITORIALE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=5490/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=guadeloupe/COLLECTIVITE_TERRITORIALE.shp']
                                }
                            }
                        },
                        'metropole': {
                            2022: {
                                'downloaded': True,
                                'paths': {
                                    'CHFLIEU_COMMUNE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/CHFLIEU_COMMUNE.shp'],
                                    'CHFLIEU_COMMUNE_ASSOCIEE_OU_DELEGUEE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/CHFLIEU_COMMUNE_ASSOCIEE_OU_DELEGUEE.shp'],
                                    'CHFLIEU_ARRONDISSEMENT_MUNICIPAL': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/CHFLIEU_ARRONDISSEMENT_MUNICIPAL.shp'],
                                    'ARRONDISSEMENT': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/ARRONDISSEMENT.shp'],
                                    'COLLECTIVITE_TERRITORIALE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/COLLECTIVITE_TERRITORIALE.shp'],
                                    'COMMUNE_ASSOCIEE_OU_DELEGUEE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/COMMUNE_ASSOCIEE_OU_DELEGUEE.shp'],
                                    'CANTON': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/CANTON.shp'],
                                    'ARRONDISSEMENT_MUNICIPAL': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/ARRONDISSEMENT_MUNICIPAL.shp'],
                                    'DEPARTEMENT': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/DEPARTEMENT.shp'],
                                    'REGION': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/REGION.shp'],
                                    'COMMUNE': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/COMMUNE.shp'],
                                    'EPCI': ['projet-cartiflette/diffusion/shapefiles-test4/year=2022/administrative_level=None/crs=2154/None=None/vectorfile_format=shp/provider=IGN/dataset_family=ADMINEXPRESS/source=EXPRESS-COG-TERRITOIRE/territory=metropole/EPCI.shp']
                                }
                            }
                        }
                    }
                }
            }
        }


    """

    if not upload:
        logger.warning("no upload to s3 will be done, set upload=True to upload")

    # Initialize MD5 json if absent
    json_md5 = f"{bucket}/{path_within_bucket}/md5.json"
    try:
        json_md5 in fs.ls(json_md5.rsplit("/", maxsplit=1)[0])
    except FileNotFoundError:
        with fs.open(json_md5, "w") as f:
            json.dump({}, f)

    kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "fs": fs,
        "upload": upload,
    }
    years = list(range(2015, date.today().year + 1))[-1::-1]

    results = {}

    logger.info("Synchronize raw sources")

    def func(args):
        key, args = args
        results = _download_sources(*args, years=years, **kwargs)
        logger.info(f"{key} done")
        return results

    datasets_args = DOWNLOAD_PIPELINE_ARGS

    if THREADS_DOWNLOAD > 1:
        with ThreadPool(THREADS_DOWNLOAD) as pool:
            iterator = pool.map(func, datasets_args.items()).result()
            while True:
                try:
                    results = deep_dict_update(results, next(iterator))
                except StopIteration:
                    break
                except Exception as e:
                    logger.error(e)
    else:
        for args in datasets_args.items():
            results = deep_dict_update(results, func(args))
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s :%(filename)s:%(lineno)d (%(funcName)s) - %(message)s",
    )

    results = download_all(upload=True)
