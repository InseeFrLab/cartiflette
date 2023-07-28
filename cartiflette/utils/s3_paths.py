# -*- coding: utf-8 -*-
import typing
from datetime import date

import cartiflette


def _vectorfile_path(
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    vectorfile_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = str(date.today().year),
    value: typing.Union[str, int, float] = "28",
    crs: typing.Union[str, int, float] = 2154,
    type_url: str = "https",
) -> str:
    """
    This function returns a file path for a vector file within a specified
    bucket (read or write on S3 storage) or the full URL (for download
    purposes). The type returned is dependant of `type_url`'s value.

    Parameters
    ----------
    bucket : str, optional
        The name of the bucket where the file will be stored. The default is
        cartiflette.BUCKET.
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
    type_url : str
        The type of requested URL. Can be either "https" for a full URL or
        "bucket" for the path in the bucket.

    Raises
    ------
    ValueError
        If type_url not among "https", "bucket".

    Returns
    -------
    str
        The complete file path for the vector file that will be used to read
        or write when interacting with S3 storage, or the URL of the vector
        file stored in S3 to be downloaded

    """

    if type_url not in ("https", "bucket"):
        msg = (
            "type_download must be either 'https' or 'bucket' - "
            f"found '{type_url}' instead"
        )
        raise ValueError(msg)

    path_within = (
        f"{bucket}/"
        f"{path_within_bucket}/"
        f"{year=}/"
        f"administrative_level={borders}/"
        f"{crs=}/"
        f"{filter_by}={value}/"
        f"{vectorfile_format=}/"
        f"{provider=}/"
        f"{source=}/"
        f"raw.{vectorfile_format}"
    )
    path_within = path_within.replace("'", "")

    if vectorfile_format == "shp":
        path_within = path_within.rsplit("/", maxsplit=1)[0] + "/"

    if type_url == "url":
        url = f"{cartiflette.ENDPOINT_URL}/{path_within}"
    else:
        url = path_within

    return url
