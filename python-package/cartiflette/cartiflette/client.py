from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse

import duckdb
import geopandas as gpd

from cartiflette.constants import (
    BUCKET,
    DEFAULT_SIMPLIFICATION,
    ENDPOINT_URL,
    GEOJSON_DEFAULT_SIMPLIFICATION,
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
        is taken from the https_proxy environment variable. Coordinates are
        read as (longitude, latitude), the order of the files, by the
        functions that depend on it (``geometry_always_xy``):
        ``ST_Distance_Sphere`` and ``ST_Transform`` from EPSG:4326 expect
        (latitude, longitude) otherwise.
    """
    global _CONNECTION
    if con is None:
        if _CONNECTION is None:
            _CONNECTION = duckdb.connect()
        con = _CONNECTION
    con.execute("SET enable_progress_bar = false")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;")
    con.execute("SET geometry_always_xy = true")
    proxy = os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY")
    if proxy:
        con.execute("SET http_proxy = ?", [urlparse(proxy).netloc or proxy])
    return con


def choose_format(
    vectorfile_format: str,
    year: int | str,
    force: bool = False,
    parquet_exists: bool | None = None,
    geojson_exists: bool | None = None,
) -> str:
    """
    Format of the files to read.

    GeoParquet is used whenever it exists (from 2025 onwards, and for the
    earlier years it has been produced for): only the needed parts of the
    file are downloaded, which is as fast as GeoJSON for a small request and
    much faster for a large one. GeoJSON is only read when there is no
    GeoParquet, or when explicitly requested with ``force=True`` for a year
    that has GeoJSON files: none is published from 2025 onwards.

    Parameters
    ----------
    vectorfile_format : str
        "auto", "geojson", "parquet" or "geoparquet".
    year : int or str
        Vintage.
    force : bool
        Read GeoJSON when requested even if GeoParquet exists, provided
        GeoJSON files exist for the year.
    parquet_exists : bool, optional
        Whether GeoParquet exists (see `parquet_available`). By default,
        deduced from the year only: from 2025 onwards.
    geojson_exists : bool, optional
        Whether GeoJSON files exist (see `geojson_available`). By default,
        deduced from the year only: before 2025.

    Returns
    -------
    str
        "parquet" or "geojson".

    Notes
    -----
    A UserWarning is emitted when GeoJSON is requested and GeoParquet exists:
    either GeoParquet is read instead (``force=False``, or no GeoJSON file
    for the year), or GeoJSON is read but is slower (``force=True``).

    Examples
    --------
    >>> choose_format("auto", 2025)
    'parquet'
    >>> choose_format("auto", 2022)
    'geojson'
    """
    if parquet_exists is None:
        parquet_exists = int(year) >= PARQUET_FIRST_YEAR
    if geojson_exists is None:
        geojson_exists = int(year) < PARQUET_FIRST_YEAR
    if vectorfile_format.lower() == "auto":
        return "parquet" if parquet_exists else "geojson"
    vectorfile_format = standardize_format(vectorfile_format)
    if vectorfile_format != "geojson" or not parquet_exists:
        return vectorfile_format
    if not geojson_exists:
        warnings.warn(
            f"GeoParquet is used instead of GeoJSON: no GeoJSON file is "
            f"published for {year} (only GeoParquet from {PARQUET_FIRST_YEAR} "
            "onwards; the cartiflette API serves GeoJSON). It gives the same "
            "result." + (" force=True is ignored." if force else ""),
            UserWarning,
            stacklevel=3,
        )
        return "parquet"
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


def consolidated_url(filter_by: str, **path_kwargs) -> str:
    """
    Link of the consolidated GeoParquet holding the polygons for `filter_by`.

    Parameters
    ----------
    filter_by : str
        Level used to select the polygons: FRANCE_ENTIERE_DROM_RAPPROCHES
        reads the file with the DROM brought closer, any other level the
        file with the true positions.
    **path_kwargs
        Other arguments of `create_path_consolidated`, except `layout`.

    Returns
    -------
    str
    """
    layout = (
        DROM_RAPPROCHES if filter_by.upper() == DROM_RAPPROCHES else "FRANCE_ENTIERE"
    )
    return f"{ENDPOINT_URL}/" + create_path_consolidated(layout=layout, **path_kwargs)


def parquet_available(filter_by: str, **path_kwargs) -> bool:
    """
    Whether a consolidated GeoParquet exists for these parameters.

    It is published for every year from 2025 onwards, so no request is made
    for them. Earlier years were first published in GeoJSON only, and
    GeoParquet may have been produced afterwards: its existence is checked
    with a HEAD request.

    Parameters
    ----------
    filter_by : str
        Level used to select the polygons.
    **path_kwargs
        Arguments of `create_path_consolidated`, except `layout`.

    Returns
    -------
    bool
    """
    if int(path_kwargs["year"]) >= PARQUET_FIRST_YEAR:
        return True
    return _exists(consolidated_url(filter_by, **path_kwargs))


def geojson_available(
    values: list[str | int | float], filter_by: str, **path_kwargs
) -> bool:
    """
    Whether GeoJSON files exist for these parameters.

    No GeoJSON file is published from 2025 onwards, so no request is made
    for them. For earlier years, the file of the first value is checked with
    a HEAD request: a year republished by the current pipeline only has
    GeoParquet.

    Parameters
    ----------
    values : list
        Values of `filter_by`.
    filter_by : str
        Level used to split the files.
    **path_kwargs
        Other arguments of `create_path_bucket`.

    Returns
    -------
    bool
    """
    if int(path_kwargs["year"]) >= PARQUET_FIRST_YEAR:
        return False
    try:
        return _exists(geojson_urls(values[:1], filter_by, **path_kwargs)[0])
    except OSError:
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


def _size(url: str) -> int:
    """Size in bytes of a file, local or remote (HEAD request)."""
    if not url.startswith(("http://", "https://")):
        if not os.path.exists(url):
            raise OSError(f"No file at {url}")
        return os.path.getsize(url)
    try:
        request = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(request) as response:
            return int(response.headers["Content-Length"])
    except urllib.error.HTTPError as e:
        raise OSError(f"No file at {url} (HTTP {e.code})") from None


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

    Raises
    ------
    OSError
        If a file does not exist.

    Notes
    -----
    A file is a single JSON object (FeatureCollection), so DuckDB's
    `maximum_object_size` must exceed the size of the largest file. It is
    set from the actual sizes (HEAD requests): DuckDB allocates buffers of
    about twice this size, so a fixed large value (e.g. 2 GB) runs out of
    memory on small machines and containers.
    """
    with ThreadPoolExecutor(max_workers=8) as pool:
        largest = max(pool.map(_size, urls))
    return con.sql(
        f"""
        SELECT
            unnest(feature.properties),
            ST_GeomFromGeoJSON(feature.geometry) AS geometry
        FROM (
            SELECT unnest(features) AS feature
            FROM read_json(
                $urls,
                maximum_object_size = {largest + 1_000_000},
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
        before 2025 only published in GeoJSON).
    ValueError
        If the file cannot be filtered by `filter_by`, or if some values are
        not found.
    """
    filter_by = filter_by.upper()
    url = consolidated_url(filter_by, **path_kwargs)
    try:
        metadata = con.execute(
            "SELECT decode(value) FROM parquet_kv_metadata($url) "
            "WHERE decode(key) = $key",
            {"url": url, "key": FILTER_COLUMNS_KEY},
        ).fetchone()
    except (duckdb.IOException, duckdb.HTTPException) as e:
        raise OSError(
            f"No GeoParquet file at {url}. GeoParquet files are available "
            f"from {PARQUET_FIRST_YEAR} onwards (and for some earlier years), "
            "use vectorfile_format='geojson' otherwise."
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

    # Relational API rather than con.sql(..., params=...), which runs the
    # query at once: the relation stays lazy, so that a later filter (`where`,
    # or SQL with engine="duckdb") is applied while reading the file
    return (
        con.read_parquet(url, hive_partitioning=False)
        .filter(
            duckdb.ColumnExpression(column).isin(
                *(duckdb.ConstantExpression(v) for v in wanted)
            )
        )
        .select(duckdb.StarExpression(exclude=["bbox"]))
    )


def filter_relation(
    relation: duckdb.DuckDBPyRelation, where: str
) -> duckdb.DuckDBPyRelation:
    """
    Keep the rows of a relation matching a DuckDB SQL expression.

    The expression can use the attributes (e.g. ``POPULATION > 2000``) and
    the `geometry` column with the functions of the DuckDB spatial extension
    (e.g. ``ST_Intersects(geometry, ST_MakeEnvelope(4.2, 43.3, 4.9, 43.8))``).
    On a GeoParquet file, DuckDB applies it while reading.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
        Relation, e.g. from `read_parquet` or `read_geojson`.
    where : str
        SQL expression, as in a ``WHERE`` clause.

    Returns
    -------
    duckdb.DuckDBPyRelation

    Raises
    ------
    ValueError
        If the expression is not valid SQL, or refers to an unknown column
        or function.
    """
    try:
        return relation.filter(where)
    except (
        duckdb.ParserException,
        duckdb.BinderException,
        duckdb.CatalogException,
    ) as e:
        raise ValueError(f"Invalid where expression {where!r}: {e}") from None


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
    where: str | None = None,
) -> gpd.GeoDataFrame | duckdb.DuckDBPyRelation | str:
    """
    Download official French borders produced by cartiflette.

    Everything is done with DuckDB; with the default engine, the result is
    converted into a GeoDataFrame at the end.

    - GeoJSON: one file per value of `filter_by`; the list of links is built
      and DuckDB reads the files together.
    - GeoParquet (from 2025 onwards, and some earlier years): one file per
      level; DuckDB filters it and only downloads the needed parts.

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
        it exists, even if "geojson" is requested, unless `force` is True;
        see `choose_format`, `parquet_available` and `geojson_available`.
    year : str or int
        Vintage of the borders. Defaults to the current year.
    crs : str or int
        EPSG code of the projection. Default 4326.
    simplification : int, optional
        Simplification level, as the percentage of points removed: 0
        (complete contours), 50, or 80 (lightest). Default 80, or 50 for the
        years only published as GeoJSON (2022), which have no 80 version.
    bucket : str
        Bucket of the files, "projet-cartiflette".
    path_within_bucket : str
        Prefix within the bucket: "production" for the published files, or a
        test location such as "test/v0.3.0".
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
        Only for years with GeoJSON files (2022): none is published from
        2025 onwards, so GeoParquet is read anyway, with a warning.
    where : str, optional
        DuckDB SQL expression to keep only some of the polygons of `values`,
        on their attributes (``"POPULATION > 2000"``) or their geometry
        (``"ST_Intersects(geometry, ST_MakeEnvelope(4.2, 43.3, 4.9, 43.8))"``,
        coordinates in `crs`); see `filter_relation`. Applied before the
        conversion, whatever the format and the engine.

    Returns
    -------
    geopandas.GeoDataFrame or duckdb.DuckDBPyRelation or str

    Raises
    ------
    ValueError
        If `vectorfile_format` or `engine` is not supported, or if `where`
        is not a valid expression; GeoParquet only: if `borders` cannot be
        filtered by `filter_by`, or if some values are not found.
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

    Communes of more than 2,000 inhabitants in Occitanie:

    >>> gdf = carti_download(
    ...     values="76",
    ...     borders="COMMUNE",
    ...     filter_by="REGION",
    ...     year=2025,
    ...     where="POPULATION > 2000",
    ... )  # doctest: +SKIP
    """
    if engine not in ENGINES:
        raise ValueError(f"engine must be one of {ENGINES}, not {engine!r}")
    if not year:
        year = datetime.now(timezone.utc).year
    if isinstance(values, (str, int, float)):
        values = [values]

    default_simplification = simplification is None
    path_kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "provider": provider,
        "dataset_family": dataset_family,
        "source": source,
        "year": year,
        "borders": borders,
        "crs": crs,
        "simplification": (
            DEFAULT_SIMPLIFICATION if default_simplification else simplification
        ),
        "filename": filename,
    }
    parquet_exists = parquet_available(filter_by, **path_kwargs)
    geojson_exists = None
    if force and parquet_exists and vectorfile_format.lower() == "geojson":
        geojson_exists = geojson_available(
            values, filter_by, territory=territory, **path_kwargs
        )
    vectorfile_format = choose_format(
        vectorfile_format,
        year,
        force,
        parquet_exists=parquet_exists,
        geojson_exists=geojson_exists,
    )
    if vectorfile_format == "geojson" and default_simplification:
        # The GeoJSON files have no 80 version
        path_kwargs["simplification"] = GEOJSON_DEFAULT_SIMPLIFICATION
    con = connect(con)
    if vectorfile_format == "parquet":
        relation = read_parquet(con, values, filter_by, **path_kwargs)
    else:
        urls = geojson_urls(values, filter_by, territory=territory, **path_kwargs)
        relation = read_geojson(con, urls)
    if where:
        relation = filter_relation(relation, where)

    if engine == "duckdb" and not return_as_json:
        return relation
    gdf = to_geopandas(relation, crs)
    return gdf.to_json() if return_as_json else gdf
