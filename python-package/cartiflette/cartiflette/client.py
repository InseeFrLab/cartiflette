from __future__ import annotations

import io
import logging
import os
import typing
from datetime import date

import geopandas as gpd
import pandas as pd
from requests_cache import CachedSession

from cartiflette.config import _config
from cartiflette.constants import (
    BUCKET,
    CACHE_NAME,
    DIR_CACHE,
    ENDPOINT_URL,
    PATH_WITHIN_BUCKET,
)
from cartiflette.utils import create_path_bucket, standardize_format

logger = logging.getLogger(__name__)


def get_session(expire_after=None) -> CachedSession:
    """
    HTTP session with a local cache. Proxies are taken from the http_proxy
    and https_proxy environment variables.
    """
    return CachedSession(
        cache_name=os.path.join(DIR_CACHE, CACHE_NAME),
        expire_after=expire_after or _config["DEFAULT_EXPIRE_AFTER"],
    )


def read_file(content: bytes, vectorfile_format: str) -> gpd.GeoDataFrame:
    if vectorfile_format == "parquet":
        return gpd.read_parquet(io.BytesIO(content))
    return gpd.read_file(io.BytesIO(content))


def download_single(
    session: CachedSession,
    value: typing.Union[str, int, float],
    vectorfile_format: str = "geojson",
    **path_kwargs,
) -> gpd.GeoDataFrame:
    """Download the file of a single value. Raises if it cannot be read."""
    path = create_path_bucket(
        value=value, vectorfile_format=vectorfile_format, **path_kwargs
    )
    url = f"{ENDPOINT_URL}/{path}"
    r = session.get(url)
    if not r.ok:
        raise IOError(f"Could not download {url} (HTTP {r.status_code})")
    return read_file(r.content, vectorfile_format)


def carti_download(
    values: typing.Union[typing.List[typing.Union[str, int, float]], str, int],
    borders: str = "COMMUNE",
    filter_by: str = "REGION",
    territory: str = "metropole",
    vectorfile_format: str = "geojson",
    year: typing.Union[str, int, None] = None,
    crs: typing.Union[str, int] = 4326,
    simplification: typing.Union[str, int, float, None] = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-CARTO-TERRITOIRE",
    filename: str = "raw",
    return_as_json: bool = False,
) -> typing.Union[gpd.GeoDataFrame, str]:
    """
    Download official French borders produced by cartiflette, for one or
    several values of `filter_by`, and concatenate them.

    Parameters
    ----------
    values : list or str or int
        Values of `filter_by` to retrieve (e.g. ["11", "84"] for REGION,
        "France" for FRANCE_ENTIERE).
    borders : str
        Level of the polygons: COMMUNE, COMMUNE_ARRONDISSEMENT, DEPARTEMENT,
        REGION, BASSIN_VIE, ZONE_EMPLOI, UNITE_URBAINE, AIRE_ATTRACTION_VILLES.
    filter_by : str
        Level used to select the polygons: DEPARTEMENT, REGION, TERRITOIRE,
        FRANCE_ENTIERE, FRANCE_ENTIERE_DROM_RAPPROCHES or a zoning.
    territory : str
        Kept for compatibility with the storage layout, "metropole".
    vectorfile_format : str
        "geojson" or "parquet".
    year : str or int
        Vintage of the borders. Defaults to the current year.
    crs : str or int
        EPSG code of the projection. Default 4326.
    simplification : int, optional
        Simplification level (percentage of points removed): 0 or 50.
    bucket, path_within_bucket, provider, dataset_family, source, filename :
        Location of the files, the defaults point to the published files.
    return_as_json : bool
        If True, return a GeoJSON string instead of a GeoDataFrame.
    """
    vectorfile_format = standardize_format(vectorfile_format)
    if not year:
        year = date.today().year
    if isinstance(values, (str, int, float)):
        values = [values]

    path_kwargs = dict(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=provider,
        dataset_family=dataset_family,
        source=source,
        year=year,
        borders=borders,
        crs=crs,
        filter_by=filter_by,
        territory=territory,
        simplification=simplification,
        filename=filename,
    )
    with get_session() as session:
        gdf = pd.concat(
            [
                download_single(session, value, vectorfile_format, **path_kwargs)
                for value in values
            ],
            ignore_index=True,
        )

    if return_as_json:
        return gdf.to_json()
    return gdf
