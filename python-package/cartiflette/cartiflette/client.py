from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pyproj
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
    create_path_consolidated,
    standardize_format,
    value_candidates,
)

logger = logging.getLogger(__name__)

DROM_RAPPROCHES = "FRANCE_ENTIERE_DROM_RAPPROCHES"
# Key of the parquet metadata mapping each level usable in `filter_by` to
# its column in a consolidated file (e.g. BASSIN_VIE -> BV2022)
FILTER_COLUMNS_KEY = b"cartiflette:filter_columns"


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


def get_s3_filesystem() -> pafs.S3FileSystem:
    """
    Anonymous access to the cartiflette storage through its S3 API.

    Unlike plain HTTP downloads, it allows reading only the needed parts of a
    parquet file. The proxy is taken from the https_proxy environment
    variable.

    Returns
    -------
    pyarrow.fs.S3FileSystem
    """
    proxy = os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY")
    endpoint = urlparse(ENDPOINT_URL)
    return pafs.S3FileSystem(
        anonymous=True,
        endpoint_override=endpoint.netloc,
        scheme=endpoint.scheme,
        region="us-east-1",
        proxy_options=proxy,
    )


def select_row_groups(
    parquet_file: pq.ParquetFile, column: str, wanted: list[str]
) -> list[int]:
    """
    Row groups of a parquet file holding some values of a column.

    Only `column` is read (a few dozen kB, as it is dictionary encoded). This
    is exact, whereas the min/max statistics of the row groups are only
    selective for the first sorting column of the file.

    Parameters
    ----------
    parquet_file : pyarrow.parquet.ParquetFile
        Opened file.
    column : str
        Column to look into.
    wanted : list of str
        Values looked for.

    Returns
    -------
    list of int
        Indices of the row groups holding at least one of the values.
    """
    values = parquet_file.read(columns=[column]).column(column)
    groups, start = [], 0
    for i in range(parquet_file.metadata.num_row_groups):
        length = parquet_file.metadata.row_group(i).num_rows
        chunk = values.slice(start, length)
        if pc.any(pc.is_in(chunk, value_set=pa.array(wanted))).as_py():
            groups.append(i)
        start += length
    return groups


def to_geodataframe(table: pa.Table) -> gpd.GeoDataFrame:
    """
    GeoDataFrame from an Arrow table read from a GeoParquet file.

    Parameters
    ----------
    table : pyarrow.Table
        Table whose schema holds the GeoParquet "geo" metadata (WKB
        geometries).

    Returns
    -------
    geopandas.GeoDataFrame
    """
    geo = json.loads(table.schema.metadata[b"geo"])
    geometry = geo["primary_column"]
    # GeoParquet: a missing "crs" means OGC:CRS84, a null one an unknown CRS
    crs = geo["columns"][geometry].get("crs", "OGC:CRS84")
    df = table.to_pandas()
    return gpd.GeoDataFrame(
        df.drop(columns=geometry),
        geometry=gpd.GeoSeries.from_wkb(
            df[geometry],
            crs=None if crs is None else pyproj.CRS.from_user_input(crs),
        ),
    )


def read_consolidated(
    fs: pafs.FileSystem,
    values: list[str | int | float],
    filter_by: str,
    **path_kwargs,
) -> gpd.GeoDataFrame:
    """
    Read the polygons of some values from a consolidated GeoParquet.

    The filter column is read first, then only the row groups holding the
    requested values are downloaded (see `select_row_groups`).

    Parameters
    ----------
    fs : pyarrow.fs.FileSystem
        Filesystem of the storage, see `get_s3_filesystem`.
    values : list
        Values of `filter_by` to retrieve.
    filter_by : str
        Level used to select the polygons (e.g. "REGION").
    **path_kwargs
        Other arguments of `create_path_consolidated`, except `geometry`,
        deduced from `filter_by`.

    Returns
    -------
    geopandas.GeoDataFrame

    Raises
    ------
    OSError
        If there is no consolidated file for these parameters (e.g. a year
        before 2025).
    ValueError
        If the file cannot be filtered by `filter_by`, or if some values are
        not found.
    """
    filter_by = filter_by.upper()
    geometry = DROM_RAPPROCHES if filter_by == DROM_RAPPROCHES else "FRANCE_ENTIERE"
    path = create_path_consolidated(geometry=geometry, **path_kwargs)
    try:
        parquet_file = pq.ParquetFile(path, filesystem=fs)
    except OSError as e:
        raise OSError(
            f"No GeoParquet file at {ENDPOINT_URL}/{path}. GeoParquet files are "
            "available from 2025 onwards, use vectorfile_format='geojson' for "
            "earlier years."
        ) from e

    filter_columns = json.loads(parquet_file.schema_arrow.metadata[FILTER_COLUMNS_KEY])
    if filter_by not in filter_columns:
        raise ValueError(
            f"{path_kwargs['borders']} polygons cannot be filtered by {filter_by}; "
            f"available: {', '.join(filter_columns)}"
        )
    column = filter_columns[filter_by]

    candidates = {str(v): value_candidates(filter_by, v) for v in values}
    wanted = list(dict.fromkeys(c for cs in candidates.values() for c in cs))
    table = parquet_file.read_row_groups(
        select_row_groups(parquet_file, column, wanted)
    )
    table = table.filter(pc.is_in(table[column], value_set=pa.array(wanted)))

    found = set(table[column].to_pylist())
    missing = [v for v, cs in candidates.items() if not found.intersection(cs)]
    if missing:
        raise ValueError(f"No {filter_by} {', '.join(missing)} in {path}")
    return to_geodataframe(table.drop_columns(["bbox"]))


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

    GeoJSON: the file of each value of `filter_by` is downloaded, and the
    files are concatenated. GeoParquet (from 2025 onwards): the polygons of the
    values are read from a single file holding the whole level, downloading
    only the needed parts.

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
        Kept for compatibility with the storage layout of the GeoJSON files,
        "metropole".
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
    ValueError
        GeoParquet only: if `borders` cannot be filtered by `filter_by`, or if
        some values are not found.

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
        "simplification": simplification,
        "filename": filename,
    }
    if vectorfile_format == "parquet":
        gdf = read_consolidated(get_s3_filesystem(), values, filter_by, **path_kwargs)
    else:
        with get_session() as session:
            gdf = pd.concat(
                [
                    download_single(
                        session,
                        value,
                        vectorfile_format,
                        filter_by=filter_by,
                        territory=territory,
                        **path_kwargs,
                    )
                    for value in values
                ],
                ignore_index=True,
            )

    if return_as_json:
        return gdf.to_json()
    return gdf
