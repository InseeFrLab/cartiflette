# -*- coding: utf-8 -*-
from datetime import date
import os
import shutil
import tempfile
import logging
import typing
import s3fs
import geopandas as gpd

from cartiflette.download.scraper import MasterScraper
from cartiflette.utils import create_path_bucket, standardize_inputs
from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS

logger = logging.getLogger(__name__)


def download_from_cartiflette_inner(
    values: typing.List[typing.Union[str, int, float]],
    borders: str = "COMMUNE",
    filter_by: str = "region",
    territory: str = "metropole",
    vectorfile_format: str = "geojson",
    year: typing.Union[str, int, float] = None,
    crs: typing.Union[list, str, int, float] = 2154,
    simplification: typing.Union[str, int, float] = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-TERRITOIRE",
    filename: str = "raw",
    return_as_json: bool = False,
    *args,
    **kwargs,
) -> typing.Union[gpd.GeoDataFrame, str]:
    """
    Downloads and aggregates official geographic datasets using the Cartiflette API
    for a set of specified values.
    Optionally returns the data as a JSON string.

    This function is useful for downloading and concatenating data related to different regions,
    communes, etc., into a single GeoDataFrame or JSON string.

    Parameters:
    - values (List[Union[str, int, float]]):
        A list of values to filter data by the filter_by parameter.
    - borders (str, optional):
        The type of borders (default is "COMMUNE").
    - filter_by (str, optional):
        The parameter to filter by (default is "region").
    - territory (str, optional):
        The territory (default is "metropole").
    - vectorfile_format (str, optional):
        The file format for vector files (default is "geojson").
    - year (Union[str, int, float], optional):
        The year of the dataset. Defaults to the current year if not provided.
    - crs (Union[list, str, int, float], optional):
        The coordinate reference system (default is 2154).
    - simplification (Union[str, int, float], optional):
        The simplification parameter (default is None).
    - bucket, path_within_bucket, provider, dataset_family, source:
        Other parameters required for accessing the Cartiflette API.

    - return_as_json (bool, optional):
        If True, the function returns a JSON string representation of the aggregated GeoDataFrame.
        If False, it returns a GeoDataFrame. Default is False.

    Returns:
    - Union[gpd.GeoDataFrame, str]:
        A GeoDataFrame containing concatenated data from the
            specified parameters if return_as_json is False.
        A JSON string representation of the GeoDataFrame
            if return_as_json is True.
    """

    # Initialize an empty list to store individual GeoDataFrames
    gdf_list = []

    # Set the year to the current year if not provided
    if not year:
        year = str(date.today().year)

    if isinstance(values, (str, int)):
        values = [values]

    # Iterate over values
    for value in values:
        gdf_single = download_cartiflette_single(
            value=value,
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            provider=provider,
            dataset_family=dataset_family,
            source=source,
            vectorfile_format=vectorfile_format,
            borders=borders,
            filter_by=filter_by,
            territory=territory,
            year=year,
            crs=crs,
            simplification=simplification,
            filename=filename,
        )
        gdf_list.append(gdf_single)

    # Concatenate the list of GeoDataFrames into a single GeoDataFrame
    concatenated_gdf = gpd.pd.concat(gdf_list, ignore_index=True)

    if return_as_json is True:
        return concatenated_gdf.to_json()

    return concatenated_gdf


def download_cartiflette_single(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-TERRITOIRE",
    vectorfile_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    territory: str = "metropole",
    year: typing.Union[str, int, float] = None,
    value: typing.Union[str, int, float] = "28",
    crs: typing.Union[list, str, int, float] = 2154,
    simplification: typing.Union[str, int, float] = None,
    filename: str = "raw",
    *args,
    **kwargs,
):
    if not year:
        year = str(date.today().year)

    corresp_filter_by_columns, format_read, driver = standardize_inputs(
        vectorfile_format
    )

    url = create_path_bucket(
        {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "vectorfile_format": format_read,
            "territory": territory,
            "borders": borders,
            "filter_by": filter_by,
            "year": year,
            "value": value,
            "crs": crs,
            "provider": provider,
            "dataset_family": dataset_family,
            "source": source,
            "simplification": simplification,
            "filename": filename
        }
    )

    url = f"https://minio.lab.sspcloud.fr/{url}"

    try:
        gdf = gpd.read_file(url)
    except Exception as e:
        logger.error(
            f"There was an error while reading the file from the URL: {url}"
        )
        logger.error(f"Error message: {str(e)}")
    else:
        return gdf


# ---------------------


def download_vectorfile_single(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-TERRITOIRE",
    vectorfile_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    territory: str = "metropole",
    year: typing.Union[str, int, float] = None,
    value: typing.Union[str, int, float] = "28",
    crs: typing.Union[list, str, int, float] = 2154,
    simplification: typing.Union[str, int, float] = None,
    type_download: str = "https",
    fs: s3fs.S3FileSystem = FS,
    *args,
    **kwargs,
) -> gpd.GeoDataFrame:
    """
    This function downloads a single vector file (from a specified S3 bucket or
    an URL) and returns it as a GeoPandas object.

    Parameters
    ----------
    bucket : str, optional
        The name of the bucket where the file is stored. The default is
        cartiflette.config.BUCKET.
    path_within_bucket : str, optional
        The path within the bucket where the file will be stored. The default
        is cartiflette.PATH_WITHIN_BUCKET.
    provider : str, optional
        Dataset's provider as described in the yaml config file. The default is
        "IGN".
    source : str, optional
        Dataset's source as described in the yaml file. The default is
        "EXPRESS-COG-TERRITOIRE".
    vectorfile_format : str, optional
        The format of the vector file, can be "geojson", "topojson", "gpkg" or
        "shp". The default is "geojson".
    borders : str, optional
        The administrative borders of the tiles within the vector file. Can be
        any administrative borders provided by IGN, e.g. "COMMUNE",
        "DEPARTEMENT" or "REGION". Default is "COMMUNE". The default is
        "COMMUNE".
    filter_by : str, optional
        The administrative borders (supra to 'borders') that will be used to
        cut the vector file in pieces when writing to S3. For instance, if
        borders is "DEPARTEMENT", filter_by can be "REGION" or
        "FRANCE_ENTIERE". The default is "region".
    year : typing.Union[str, int, float], optional
        The year of the vector file. The default is the current date's year.
    value : typing.Union[str, int, float], optional
        The value of the vector file (associated to the `filter_by` argument).
        The default is "28".
    crs : typing.Union[str, int, float], optional
        The coordinate reference system of the vector file. The default is
        2154.
    type_download : str, optional
        The download's type to perform. Can be either "https" or "bucket".
        The default is "https".
    fs : s3fs.S3FileSystem, optional
        The s3 file system to use (in case of "bucket" download type). The
        default is cartiflette.FS.
    *args
        Arguments passed to requests.Session (in case of "https" download type)
    **kwargs
        Arguments passed to requests.Session (in case of "https" download type)

    Raises
    ------
    ValueError
        If type_download not among "https", "bucket".

    Returns
    -------
    gdf : gpd.GeoDataFrame
        The vector file as a GeoPandas object

    """
    if not year:
        year = str(date.today().year)

    corresp_filter_by_columns, format_read, driver = standardize_inputs(
        vectorfile_format
    )

    if type_download not in ("https", "bucket"):
        msg = (
            "type_download must be either 'https' or 'bucket' - "
            f"found '{type_download}' instead"
        )
        raise ValueError(msg)

    url = create_path_bucket(
        {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "vectorfile_format": format_read,
            "territory": territory,
            "borders": borders,
            "filter_by": filter_by,
            "year": year,
            "value": value,
            "crs": crs,
            "provider": provider,
            "dataset_family": dataset_family,
            "source": source,
            "simplification": simplification,
        }
    )

    if type_download == "bucket":
        try:
            fs.exists(url)
        except Exception:
            raise IOError(f"File has not been found at path {url} on S3")

        if format_read == "shp":
            tdir = tempfile.TemporaryDirectory()
            files = fs.ls(url)
            for remote_file in files:
                local_path = f"{tdir.name}/{remote_file.replace(url, '')}"
                fs.download(remote_file, local_path)
            local_path = f"{tdir.name}/raw.shp"

        else:
            tfile = tempfile.TemporaryFile()
            local_path = tfile.name
            fs.download(remote_file, local_path)

    else:
        with MasterScraper(*args, **kwargs) as s:
            # Note that python should cleanup all tmpfile by itself

            if format_read == "shp":
                tdir = tempfile.TemporaryDirectory()
                for ext in ["cpg", "dbf", "prj", "shp", "shx"]:
                    successes = []
                    for remote_file in files:
                        remote = os.path.splitext(url)[0] + f".{ext}"
                        success, tmp = s.download_to_tempfile_http(
                            url=remote
                        )
                        successes.append(success)
                        shutil.copy(tmp, f"{tdir.name}/raw.{ext}")
                    local_path = f"{tdir.name}/raw.shp"
                    success = any(successes)
            else:
                success, local_path = s.download_to_tempfile_http(url=url)

        if not success:
            raise IOError("Download failed")

    if format_read == "parquet":
        gdf = gpd.read_parquet(local_path)
    else:
        gdf = gpd.read_file(local_path, driver=driver)

    # Cleanup
    try:
        os.unlink(local_path)
    except Exception:
        pass
    try:
        shutil.rmtree(tdir)
    except Exception:
        pass

    return gdf


def download_vectorfile_multiple(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    vectorfile_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = None,
    values: typing.Union[list, str, int, float] = "28",
    crs: typing.Union[list, str, int, float] = 2154,
    type_download: str = "https",
    fs: s3fs.S3FileSystem = FS,
    *args,
    **kwargs,
) -> gpd.GeoDataFrame:
    """
    This function performs multiple downloads of individual vector files (from
    a specified S3 bucket or an URL) and returns their concatenation as a
    GeoPandas object.

    Parameters
    ----------
    bucket : str, optional
        The name of the bucket where the file is stored. The default is
        cartiflette.config.BUCKET.
    path_within_bucket : str, optional
        The path within the bucket where the file will be stored. The default
        is cartiflette.PATH_WITHIN_BUCKET.
    provider : str, optional
        Dataset's provider as described in the yaml config file. The default is
        "IGN".
    source : str, optional
        Dataset's source as described in the yaml file. The default is
        "EXPRESS-COG-TERRITOIRE".
    vectorfile_format : str, optional
        The format of the vector file, can be "geojson", "topojson", "gpkg" or
        "shp". The default is "geojson".
    borders : str, optional
        The administrative borders of the tiles within the vector file. Can be
        any administrative borders provided by IGN, e.g. "COMMUNE",
        "DEPARTEMENT" or "REGION". Default is "COMMUNE". The default is
        "COMMUNE".
    filter_by : str, optional
        The administrative borders (supra to 'borders') that will be used to
        cut the vector file in pieces when writing to S3. For instance, if
        borders is "DEPARTEMENT", filter_by can be "REGION" or
        "FRANCE_ENTIERE". The default is "region".
    year : typing.Union[str, int, float], optional
        The year of the vector file. The default is the current date's year.
    values : typing.Union[list, str, int, float], optional
        The values of the vector files (associated to the `filter_by`
        argument). In case of multiple files, a concatenation will be
        performed. The default is "28".
    crs : typing.Union[str, int, float], optional
        The coordinate reference system of the vector file. The default is
        2154.
    type_download : str, optional
        The download's type to perform. Can be either "https" or "bucket".
        The default is "https".
    fs : s3fs.S3FileSystem, optional
        The s3 file system to use (in case of "bucket" download type). The
        default is cartiflette.FS.
    *args
        Arguments passed to requests.Session (in case of "https" download type)
    **kwargs
        Arguments passed to requests.Session (in case of "https" download type)

    Raises
    ------
    ValueError
        If type_download not among "https", "bucket".

    Returns
    -------
    gdf : gpd.GeoDataFrame
        The vector file as a GeoPandas object

    """

    if not year:
        year = str(date.today().year)

    if isinstance(values, (str, int, float)):
        values = [str(values)]

    if type_download not in ("https", "bucket"):
        msg = (
            "type_download must be either 'https' or 'bucket' - "
            f"found '{type_download}' instead"
        )
        raise ValueError(msg)

    vectors = [
        download_vectorfile_single(
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            provider=provider,
            source=source,
            vectorfile_format=vectorfile_format,
            borders=borders,
            filter_by=filter_by,
            year=year,
            value=val,
            crs=crs,
            type_download=type_download,
            fs=fs,
            *args**kwargs,
        )
        for val in values
    ]

    vectors = gpd.pd.concat(vectors)

    return vectors
