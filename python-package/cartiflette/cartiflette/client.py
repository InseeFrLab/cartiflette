from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import warnings
from datetime import datetime, timezone
from urllib.parse import urlparse

import duckdb
import geopandas as gpd

from cartiflette.constants import (
    BUCKET,
    ENDPOINT_URL,
    PARQUET_FIRST_YEAR,
    PATH_WITHIN_BUCKET,
)
from cartiflette.utils import (
    create_path_bucket,
    create_path_consolidated,
    standardize_format,
    value_candidates,
)

DROM_RAPPROCHES = "FRANCE_ENTIERE_DROM_RAPPROCHES"
ENGINES = ("geopandas", "duckdb")
# Key of the GeoParquet metadata mapping each level usable in `filter_by` to
# its column in a consolidated file (e.g. BASSIN_VIE -> BV2022)
FILTER_COLUMNS_KEY = "cartiflette:filter_columns"


_CONNECTION: duckdb.DuckDBPyConnection | None = None


def connect(con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyConnection:
    """
    DuckDB connection able to read the cartiflette files.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection, optional
        Existing connection to use. By default, an in-memory connection
        shared by all the calls: the relations returned with
        ``engine="duckdb"`` stay usable, and the extensions are loaded once.

    Returns
    -------
    duckdb.DuckDBPyConnection
        Connection with the httpfs and spatial extensions loaded. The proxy
        is taken from the https_proxy environment variable.
    """
    global _CONNECTION
    if con is None:
        if _CONNECTION is None:
            _CONNECTION = duckdb.connect()
        con = _CONNECTION
    con.execute("SET enable_progress_bar = false")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;")
    proxy = os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY")
    if proxy:
        con.execute("SET http_proxy = ?", [urlparse(proxy).netloc or proxy])
    return con


def choose_format(vectorfile_format: str, year: int | str, force: bool = False) -> str:
    """
    Format of the files to read.

    GeoParquet is used whenever it exists (from 2025 onwards): only the needed
    parts of the file are downloaded, which is as fast as GeoJSON for a small
    request and much faster for a large one. GeoJSON is only read for earlier
    years, or when explicitly requested with ``force=True``.

    Parameters
    ----------
    vectorfile_format : str
        "auto", "geojson", "parquet" or "geoparquet".
    year : int or str
        Vintage.
    force : bool
        Read GeoJSON when requested even if GeoParquet exists.

    Returns
    -------
    str
        "parquet" or "geojson".

    Notes
    -----
    A UserWarning is emitted when GeoJSON is requested and GeoParquet exists:
    either GeoParquet is read instead (``force=False``), or GeoJSON is read
    but is slower (``force=True``).

    Examples
    --------
    >>> choose_format("auto", 2025)
    'parquet'
    >>> choose_format("auto", 2022)
    'geojson'
    """
    parquet_exists = int(year) >= PARQUET_FIRST_YEAR
    if vectorfile_format.lower() == "auto":
        return "parquet" if parquet_exists else "geojson"
    vectorfile_format = standardize_format(vectorfile_format)
    if vectorfile_format != "geojson" or not parquet_exists:
        return vectorfile_format
    if force:
        warnings.warn(
            f"Reading GeoJSON files as requested (force=True). GeoParquet is "
            f"available for {year}: it gives the same result and is much faster "
            "for large requests, as only the needed parts are downloaded.",
            UserWarning,
            stacklevel=3,
        )
        return "geojson"
    warnings.warn(
        f"GeoParquet is used instead of GeoJSON, as it is available for {year}: "
        "it gives the same result and is much faster for large requests, as only "
        "the needed parts are downloaded. Pass force=True to read the GeoJSON "
        "files anyway.",
        UserWarning,
        stacklevel=3,
    )
    return "parquet"


def _exists(url: str) -> bool:
    if not url.startswith(("http://", "https://")):
        return os.path.exists(url)
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD")):
            return True
    except urllib.error.HTTPError:
        return False


def geojson_urls(
    values: list[str | int | float], filter_by: str, **path_kwargs
) -> list[str]:
    """
    Links of the GeoJSON files of some values, one file per value.

    Parameters
    ----------
    values : list
        Values of `filter_by`.
    filter_by : str
        Level used to split the files (e.g. "REGION").
    **path_kwargs
        Other arguments of `create_path_bucket`.

    Returns
    -------
    list of str

    Raises
    ------
    OSError
        If no file exists for a value that has several possible spellings
        (see `value_candidates`).
    """
    urls = []
    for value in values:
        candidates = [
            f"{ENDPOINT_URL}/"
            + create_path_bucket(
                filter_by=filter_by,
                value=candidate,
                vectorfile_format="geojson",
                **path_kwargs,
            )
            for candidate in value_candidates(filter_by, value)
        ]
        if len(candidates) == 1:
            # A missing file makes DuckDB raise an explicit HTTP 404 error
            urls.append(candidates[0])
            continue
        existing = [url for url in candidates if _exists(url)]
        if not existing:
            raise OSError(
                f"No file for {filter_by} {value}: {' nor '.join(candidates)}"
            )
        urls.append(existing[0])
    return urls


def read_geojson(
    con: duckdb.DuckDBPyConnection, urls: list[str]
) -> duckdb.DuckDBPyRelation:
    """
    Relation over several GeoJSON files, one row per feature.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        See `connect`.
    urls : list of str
        Links of the files.

    Returns
    -------
    duckdb.DuckDBPyRelation
        The properties of the features as columns, and a `geometry` column.
    """
    return con.sql(
        """
        SELECT
            unnest(feature.properties),
            ST_GeomFromGeoJSON(feature.geometry) AS geometry
        FROM (
            SELECT unnest(features) AS feature
            FROM read_json(
                $urls,
                maximum_object_size = 2000000000,
                union_by_name = true,
                hive_partitioning = false
            )
        )
        """,
        params={"urls": urls},
    )


def read_parquet(
    con: duckdb.DuckDBPyConnection,
    values: list[str | int | float],
    filter_by: str,
    **path_kwargs,
) -> duckdb.DuckDBPyRelation:
    """
    Relation over the polygons of some values in a consolidated GeoParquet.

    The file holds a whole level and the filter is a `WHERE` clause: DuckDB
    only downloads the row groups holding the requested values, thanks to
    the sorting of the file, its statistics and bloom filters.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        See `connect`.
    values : list
        Values of `filter_by` to retrieve.
    filter_by : str
        Level used to select the polygons (e.g. "REGION").
    **path_kwargs
        Other arguments of `create_path_consolidated`, except `layout`,
        deduced from `filter_by`.

    Returns
    -------
    duckdb.DuckDBPyRelation

    Raises
    ------
    OSError
        If there is no GeoParquet file for these parameters (e.g. a year
        before 2025).
    ValueError
        If the file cannot be filtered by `filter_by`, or if some values are
        not found.
    """
    filter_by = filter_by.upper()
    layout = DROM_RAPPROCHES if filter_by == DROM_RAPPROCHES else "FRANCE_ENTIERE"
    url = f"{ENDPOINT_URL}/" + create_path_consolidated(layout=layout, **path_kwargs)
    try:
        metadata = con.execute(
            "SELECT decode(value) FROM parquet_kv_metadata($url) "
            "WHERE decode(key) = $key",
            {"url": url, "key": FILTER_COLUMNS_KEY},
        ).fetchone()
    except (duckdb.IOException, duckdb.HTTPException) as e:
        raise OSError(
            f"No GeoParquet file at {url}. GeoParquet files are available "
            f"from {PARQUET_FIRST_YEAR} onwards, use vectorfile_format='geojson' "
            "for earlier years."
        ) from e
    filter_columns = json.loads(metadata[0])
    if filter_by not in filter_columns:
        raise ValueError(
            f"{path_kwargs['borders']} polygons cannot be filtered by {filter_by}; "
            f"available: {', '.join(filter_columns)}"
        )
    column = filter_columns[filter_by]

    candidates = {str(v): value_candidates(filter_by, v) for v in values}
    wanted = list(dict.fromkeys(c for cs in candidates.values() for c in cs))
    source = "read_parquet($url, hive_partitioning = false)"
    where = f'"{column}" IN (SELECT unnest($wanted))'
    params = {"url": url, "wanted": wanted}

    found = {
        row[0]
        for row in con.execute(
            f'SELECT DISTINCT "{column}" FROM {source} WHERE {where}', params
        ).fetchall()
    }
    missing = [v for v, cs in candidates.items() if not found.intersection(cs)]
    if missing:
        raise ValueError(f"No {filter_by} {', '.join(missing)} in {url}")

    return con.sql(
        f"SELECT * EXCLUDE (bbox) FROM {source} WHERE {where}", params=params
    )


def to_geopandas(relation: duckdb.DuckDBPyRelation, crs: int | str) -> gpd.GeoDataFrame:
    """
    Convert a relation with a `geometry` column into a GeoDataFrame.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
        Relation, e.g. from `read_parquet` or `read_geojson`.
    crs : int or str
        EPSG code of the geometries.

    Returns
    -------
    geopandas.GeoDataFrame
    """
    table = relation.select(
        "* EXCLUDE (geometry), ST_AsWKB(geometry) AS geometry"
    ).to_arrow_table()
    df = table.to_pandas()
    return gpd.GeoDataFrame(
        df.drop(columns="geometry"),
        geometry=gpd.GeoSeries.from_wkb(df["geometry"], crs=f"EPSG:{crs}"),
    )


def carti_download(
    values: list[str | int | float] | str | int,
    borders: str = "COMMUNE",
    filter_by: str = "REGION",
    territory: str = "metropole",
    vectorfile_format: str = "auto",
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
    engine: str = "geopandas",
    con: duckdb.DuckDBPyConnection | None = None,
    force: bool = False,
) -> gpd.GeoDataFrame | duckdb.DuckDBPyRelation | str:
    """
    Download official French borders produced by cartiflette.

    Everything is done with DuckDB; with the default engine, the result is
    converted into a GeoDataFrame at the end.

    - GeoJSON: one file per value of `filter_by`; the list of links is built
      and DuckDB reads the files together.
    - GeoParquet (from 2025 onwards): one file per level; DuckDB filters it
      and only downloads the needed parts.

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
        "auto" (default), "geojson" or "parquet". GeoParquet is read whenever
        it exists (from 2025 onwards), even if "geojson" is requested, unless
        `force` is True; see `choose_format`.
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
        If True, return a GeoJSON string.
    engine : str
        "geopandas" (default): return a GeoDataFrame. "duckdb": return the
        DuckDB relation, not evaluated yet, to go on with SQL.
    con : duckdb.DuckDBPyConnection, optional
        DuckDB connection to use (e.g. to join the result with other data).
        By default, an in-memory connection shared by all the calls.
    force : bool
        Read GeoJSON when ``vectorfile_format="geojson"`` even if GeoParquet
        exists for the year. A warning recalls that GeoParquet is faster.

    Returns
    -------
    geopandas.GeoDataFrame or duckdb.DuckDBPyRelation or str

    Raises
    ------
    ValueError
        If `vectorfile_format` or `engine` is not supported; GeoParquet only:
        if `borders` cannot be filtered by `filter_by`, or if some values are
        not found.
    OSError
        If a file cannot be found.

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

    Communes of two regions, kept in DuckDB:

    >>> rel = carti_download(
    ...     values=["11", "84"],
    ...     borders="COMMUNE",
    ...     filter_by="REGION",
    ...     year=2025,
    ...     engine="duckdb",
    ... )
    >>> rel.aggregate("INSEE_REG, sum(POPULATION)")  # doctest: +SKIP
    """
    if engine not in ENGINES:
        raise ValueError(f"engine must be one of {ENGINES}, not {engine!r}")
    if not year:
        year = datetime.now(timezone.utc).year
    vectorfile_format = choose_format(vectorfile_format, year, force)
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
    con = connect(con)
    if vectorfile_format == "parquet":
        relation = read_parquet(con, values, filter_by, **path_kwargs)
    else:
        urls = geojson_urls(values, filter_by, territory=territory, **path_kwargs)
        relation = read_geojson(con, urls)

    if engine == "duckdb" and not return_as_json:
        return relation
    gdf = to_geopandas(relation, crs)
    return gdf.to_json() if return_as_json else gdf
