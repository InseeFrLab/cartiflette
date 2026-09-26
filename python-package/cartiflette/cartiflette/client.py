from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timezone

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
from cartiflette.utils import (
    create_path_bucket,
    standardize_format,
    value_candidates,
)

logger = logging.getLogger(__name__)


def get_session(expire_after=None) -> CachedSession:
    """
    HTTP session with a local cache.

    Proxies are taken from the http_proxy and https_proxy environment
    variables.

    Parameters
    ----------
    expire_after : datetime.timedelta, optional
        Lifetime of the cached responses. Defaults to 30 days.

    Returns
    -------
    requests_cache.CachedSession
        Session caching responses in the user cache directory
        (``platformdirs.user_cache_dir("cartiflette")``).
    """
    return CachedSession(
        cache_name=os.path.join(DIR_CACHE, CACHE_NAME),
        expire_after=expire_after or _config["DEFAULT_EXPIRE_AFTER"],
    )


def read_file(content: bytes, vectorfile_format: str) -> gpd.GeoDataFrame:
    """
    Read a downloaded file into a GeoDataFrame.

    Parameters
    ----------
    content : bytes
        Raw content of the file.
    vectorfile_format : str
        "parquet" (GeoParquet) or "geojson".

    Returns
    -------
    geopandas.GeoDataFrame
    """
    if vectorfile_format == "parquet":
        return gpd.read_parquet(io.BytesIO(content))
    return gpd.read_file(io.BytesIO(content))


def download_single(
    session: CachedSession,
    value: str | float,
    vectorfile_format: str = "geojson",
    **path_kwargs,
) -> gpd.GeoDataFrame:
    """
    Download the file of a single value.

    Several spellings of the value may be tried (see `value_candidates`).

    Parameters
    ----------
    session : requests_cache.CachedSession
        Session used for the requests, see `get_session`.
    value : str or float
        Value of `filter_by` (e.g. "11" for a region).
    vectorfile_format : str
        "geojson" or "parquet".
    **path_kwargs
        Other arguments of `create_path_bucket`, `filter_by` included.

    Returns
    -------
    geopandas.GeoDataFrame

    Raises
    ------
    OSError
        If no candidate file can be downloaded.
    """
    urls = []
    for candidate in value_candidates(path_kwargs["filter_by"], value):
        path = create_path_bucket(
            value=candidate, vectorfile_format=vectorfile_format, **path_kwargs
        )
        urls.append(f"{ENDPOINT_URL}/{path}")
        r = session.get(urls[-1])
        if r.ok:
            return read_file(r.content, vectorfile_format)
    raise OSError(f"Could not download {' nor '.join(urls)} (HTTP {r.status_code})")


def carti_download(
    values: list[str | int | float] | str | int,
    borders: str = "COMMUNE",
    filter_by: str = "REGION",
    territory: str = "metropole",
    vectorfile_format: str = "geojson",
    year: str | int | None = None,
    crs: str | int = 4326,
    simplification: str | float | None = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-CARTO-TERRITOIRE",
    filename: str = "raw",
    return_as_json: bool = False,
) -> gpd.GeoDataFrame | str:
    """
    Download official French borders produced by cartiflette.

    The files of one or several values of `filter_by` are downloaded and
    concatenated.

    Parameters
    ----------
    values : list or str or int
        Values of `filter_by` to retrieve (e.g. ["11", "84"] for REGION,
        "France" for FRANCE_ENTIERE). Region codes below 10 can be given as
        1, "1" or "01".
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
    bucket : str
        Bucket of the files, "projet-cartiflette".
    path_within_bucket : str
        Prefix within the bucket: "production" for the published files, or a
        test location such as "test/v0.2.0".
    provider : str
        Provider of the data, "IGN".
    dataset_family : str
        Dataset family, "ADMINEXPRESS".
    source : str
        Source label in the paths, "EXPRESS-COG-CARTO-TERRITOIRE".
    filename : str
        Name of the file without extension, "raw".
    return_as_json : bool
        If True, return a GeoJSON string instead of a GeoDataFrame.

    Returns
    -------
    geopandas.GeoDataFrame or str
        The polygons of all the requested values, concatenated (a GeoJSON
        string if `return_as_json`).

    Raises
    ------
    ValueError
        If `vectorfile_format` is not supported.
    OSError
        If the file of one of the values cannot be downloaded.

    Examples
    --------
    Departements with the DROM brought closer to metropolitan France:

    >>> from cartiflette import carti_download
    >>> gdf = carti_download(
    ...     values=["France"],
    ...     borders="DEPARTEMENT",
    ...     filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
    ...     simplification=50,
    ...     year=2022,
    ... )

    Communes of two regions, as GeoParquet:

    >>> gdf = carti_download(
    ...     values=["11", "84"],
    ...     borders="COMMUNE",
    ...     filter_by="REGION",
    ...     vectorfile_format="parquet",
    ...     year=2025,
    ... )
    """
    vectorfile_format = standardize_format(vectorfile_format)
    if not year:
        year = datetime.now(timezone.utc).year
    if isinstance(values, (str, int, float)):
        values = [values]

    path_kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "provider": provider,
        "dataset_family": dataset_family,
        "source": source,
        "year": year,
        "borders": borders,
        "crs": crs,
        "filter_by": filter_by,
        "territory": territory,
        "simplification": simplification,
        "filename": filename,
    }
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
