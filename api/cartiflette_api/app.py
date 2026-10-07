"""
GeoJSON API over the consolidated GeoParquet files.

The pipeline publishes one GeoParquet file per level; this API reads it with
the cartiflette client (DuckDB only downloads the row groups of the requested
values) and serializes the polygons as GeoJSON in SQL, streamed feature by
feature. It removes the need to publish one GeoJSON file per value.

Two routes:

- ``/v1/geojson``: query parameters, several values in one response.
- ``/v1/geoparquet``: same parameters, a GeoParquet file to download.
- ``/{legacy GeoJSON path}``: the path of a GeoJSON file in the S3 storage
  (``projet-cartiflette/production/provider=IGN/.../raw.geojson``), so that the
  clients reading those files only change the host.

When there is no GeoParquet for the year (2022), both routes redirect to the
GeoJSON file; ``/v1/geojson`` with several values merges the files instead.

``where`` filters the polygons with a DuckDB SQL expression, as in
`carti_download`, before they are serialized. It is checked first
(`check_where`): one expression, no subquery, and only some functions, so
that it cannot read anything but the requested polygons.

Run with ``uvicorn --factory cartiflette_api.app:create_app``.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Annotated

import duckdb
from cartiflette import client
from cartiflette.constants import (
    BUCKET,
    DEFAULT_SIMPLIFICATION,
    GEOJSON_DEFAULT_SIMPLIFICATION,
    PATH_WITHIN_BUCKET,
)
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse
from starlette.background import BackgroundTask

MEDIA_TYPE = "application/geo+json"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
# Downloaded files are named after the level and the year (e.g. DEP2026),
# like the Insee files, rather than "raw" as in the storage
FILE_PREFIXES = {
    "COMMUNE": "COM",
    "COMMUNE_ARRONDISSEMENT": "COMARM",
    "DEPARTEMENT": "DEP",
    "REGION": "REG",
    "BASSIN_VIE": "BV",
    "ZONE_EMPLOI": "ZE",
    "UNITE_URBAINE": "UU",
    "AIRE_ATTRACTION_VILLES": "AAV",
}
# Features fetched from DuckDB and sent at once
BATCH_SIZE = 500
# Same labels as the published files (see create_path_bucket)
DEFAULT_PATH_KWARGS = {
    "bucket": BUCKET,
    "provider": "IGN",
    "dataset_family": "ADMINEXPRESS",
    "source": "EXPRESS-COG-CARTO-TERRITOIRE",
    "filename": "raw",
}
# Prefix within the bucket: "production" or e.g. "test/v0.3.0", no ".."
PATH_WITHIN_BUCKET_PATTERN = re.compile(r"[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*")
# What a `where` expression may contain (see check_where): nodes of the
# DuckDB syntax tree, and functions besides the operators and the spatial
# ones (st_*)
WHERE_NODES = {
    "BETWEEN",
    "CASE",
    "CAST",
    "COLUMN_REF",
    "COMPARISON",
    "CONJUNCTION",
    "CONSTANT",
    "FUNCTION",
    "OPERATOR",
}
WHERE_FUNCTIONS = {
    "abs",
    "ceil",
    "coalesce",
    "concat",
    "contains",
    "ends_with",
    "floor",
    "greatest",
    "least",
    "left",
    "length",
    "lower",
    "nullif",
    "prefix",
    "regexp_matches",
    "replace",
    "right",
    "round",
    "sqrt",
    "starts_with",
    "strip_accents",
    "strlen",
    "substr",
    "substring",
    "suffix",
    "trim",
    "upper",
}
# Errors of an expression on the values of the rows (e.g. a failed cast)
WHERE_ERRORS = (
    duckdb.ConversionException,
    duckdb.InvalidInputException,
    duckdb.OutOfRangeException,
)
# Path of a GeoJSON file, as built by create_path_bucket
LEGACY_PATH = re.compile(
    r"(?P<bucket>[^/]+)/(?P<path_within_bucket>.+?)"
    r"/provider=(?P<provider>[^/]+)"
    r"/dataset_family=(?P<dataset_family>[^/]+)"
    r"/source=(?P<source>[^/]+)"
    r"/year=(?P<year>\d{4})"
    r"/administrative_level=(?P<borders>[A-Z_]+)"
    r"/crs=(?P<crs>\d+)"
    r"/(?P<filter_by>[A-Z_]+)=(?P<value>[^/]+)"
    r"/vectorfile_format=geojson"
    r"/territory=[^/]+"
    r"/simplification=(?P<simplification>\d+)"
    r"/(?P<filename>[^/]+)\.geojson"
)


def features_sql(relation: duckdb.DuckDBPyRelation) -> str:
    """
    Query serializing each row of `relation` as a GeoJSON Feature.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
        Relation with a `geometry` column, registered as ``rel``.

    Returns
    -------
    str
        Query returning one VARCHAR column, one Feature per row.
    """
    properties = ", ".join(
        f'"{c}" := "{c}"' for c in relation.columns if c != "geometry"
    )
    return (
        """SELECT '{"type":"Feature","properties":' """
        f"|| to_json(struct_pack({properties})) "
        """|| ',"geometry":' || ST_AsGeoJSON(geometry) || '}' FROM rel"""
    )


def stream_geojson(
    cursor: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    headers: dict | None = None,
) -> StreamingResponse:
    """
    GeoJSON FeatureCollection of a relation, streamed.

    The first rows are read before answering, so that a missing file gives a
    404 rather than a truncated response.

    Parameters
    ----------
    cursor : duckdb.DuckDBPyConnection
        Cursor of `relation`, closed once the response is sent.
    relation : duckdb.DuckDBPyRelation
        Relation with a `geometry` column in EPSG:4326 (RFC 7946).
    headers : dict, optional
        Headers of the response (e.g. `content_disposition`).

    Returns
    -------
    StreamingResponse

    Raises
    ------
    HTTPException
        404 if a file cannot be read.
    """
    features = relation.query("rel", features_sql(relation))
    try:
        rows = features.fetchmany(BATCH_SIZE)
    except (duckdb.IOException, duckdb.HTTPException) as e:
        cursor.close()
        raise HTTPException(404, str(e)) from None
    except WHERE_ERRORS as e:
        cursor.close()
        raise HTTPException(400, str(e)) from None

    def chunks(rows: list) -> Iterator[str]:
        try:
            yield '{"type":"FeatureCollection","features":['
            separator = ""
            while rows:
                yield separator + ",".join(row[0] for row in rows)
                separator = ","
                rows = features.fetchmany(BATCH_SIZE)
            yield "]}"
        finally:
            cursor.close()

    return StreamingResponse(chunks(rows), media_type=MEDIA_TYPE, headers=headers)


def file_name(borders: str, year: int | str, extension: str) -> str:
    """
    Name of a downloaded file, e.g. DEP2026.geojson.

    Parameters
    ----------
    borders : str
        Level of the polygons (e.g. "DEPARTEMENT").
    year : int or str
        Vintage.
    extension : str
        "geojson" or "parquet".

    Returns
    -------
    str
    """
    prefix = FILE_PREFIXES.get(borders.upper(), borders.upper())
    return f"{prefix}{year}.{extension}"


def content_disposition(name: str, download: bool) -> dict:
    """Header naming the file: downloaded (attachment) or shown (inline)."""
    return {
        "Content-Disposition": f'{"attachment" if download else "inline"}; filename="{name}"'
    }


def check_path_within_bucket(path_within_bucket: str) -> str:
    if not PATH_WITHIN_BUCKET_PATTERN.fullmatch(path_within_bucket) or (
        ".." in path_within_bucket.split("/")
    ):
        raise HTTPException(400, f"Invalid path_within_bucket {path_within_bucket!r}")
    return path_within_bucket


def _syntax_nodes(node) -> Iterator[dict]:
    """Expression nodes of a DuckDB syntax tree (json_serialize_sql)."""
    if isinstance(node, dict):
        if "class" in node:
            yield node
        for value in node.values():
            yield from _syntax_nodes(value)
    elif isinstance(node, list):
        for value in node:
            yield from _syntax_nodes(value)


def check_where(con: duckdb.DuckDBPyConnection, where: str) -> str:
    """
    Check that a `where` parameter is a harmless expression.

    It is parsed by DuckDB (``json_serialize_sql``) as ``SELECT <where>``,
    the way `DuckDBPyRelation.filter` parses it: one expression, without
    ``FROM``, subquery or other clause, made of columns, constants,
    operators, ``CASE``, ``CAST``, the spatial functions (``ST_*``) and the
    functions of `WHERE_FUNCTIONS`. Table functions (``read_text``,
    ``ST_Read``...) can only be called in a subquery or a ``FROM``, so the
    expression cannot read anything else than the requested polygons.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection used to parse the expression.
    where : str
        Expression.

    Returns
    -------
    str
        `where`, unchanged.

    Raises
    ------
    HTTPException
        400 if the expression is not allowed.
    """
    parsed = json.loads(
        con.execute(
            "SELECT json_serialize_sql($q)", {"q": f"SELECT {where}"}
        ).fetchone()[0]
    )
    if parsed["error"]:
        raise HTTPException(400, f"Invalid where: {parsed['error_message']}")
    statements = parsed["statements"]
    node = statements[0]["node"] if len(statements) == 1 else {}
    if (
        node.get("type") != "SELECT_NODE"
        or len(node["select_list"]) != 1
        or node["from_table"]["type"] != "EMPTY"
        or node["cte_map"]["map"]
        or any(
            node[key]
            for key in (
                "where_clause",
                "group_expressions",
                "having",
                "qualify",
                "sample",
                "modifiers",
            )
        )
    ):
        raise HTTPException(400, "Invalid where: expected a single expression")
    for expression in _syntax_nodes(node["select_list"]):
        if expression["class"] not in WHERE_NODES:
            raise HTTPException(
                400, f"Invalid where: {expression['class'].lower()} not allowed"
            )
        if expression["class"] != "FUNCTION" or expression["is_operator"]:
            continue
        name = expression["function_name"].lower()
        if (
            expression["schema"]
            or expression["catalog"]
            or not (name.startswith("st_") or name in WHERE_FUNCTIONS)
        ):
            raise HTTPException(400, f"Invalid where: function {name} not allowed")
    return where


def filter_relation(
    cursor: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    where: str | None,
) -> duckdb.DuckDBPyRelation:
    """
    `client.filter_relation` of a checked `where`; the cursor is closed on
    error.

    Raises
    ------
    HTTPException
        400 if the expression is not allowed or not valid.
    """
    if not where:
        return relation
    try:
        return client.filter_relation(relation, check_where(cursor, where))
    except ValueError as e:
        cursor.close()
        raise HTTPException(400, str(e)) from None
    except HTTPException:
        cursor.close()
        raise


@lru_cache(maxsize=1024)
def parquet_available(filter_by: str, path_kwargs: tuple) -> bool:
    """`client.parquet_available`, cached: a HEAD request before 2025."""
    return client.parquet_available(filter_by, **dict(path_kwargs))


def parquet_response(
    con: duckdb.DuckDBPyConnection,
    values: list[str],
    filter_by: str,
    path_kwargs: dict,
    headers: dict | None = None,
    where: str | None = None,
) -> StreamingResponse:
    """
    GeoJSON of the polygons of some values, read from the GeoParquet.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection of the application; a cursor is used for the request.
    values : list of str
        Values of `filter_by`.
    filter_by : str
        Level used to select the polygons.
    path_kwargs : dict
        Arguments of `create_path_consolidated`, except `layout`.
    headers : dict, optional
        Headers of the response.
    where : str, optional
        DuckDB SQL expression filtering the polygons (see `check_where`).

    Returns
    -------
    StreamingResponse

    Raises
    ------
    HTTPException
        404 if there is no GeoParquet file, if it cannot be filtered by
        `filter_by` or if some values are not found; 400 if `where` is not
        allowed or not valid.
    """
    cursor, relation = read_parquet(con, values, filter_by, path_kwargs, where)
    return stream_geojson(cursor, relation, headers)


def read_parquet(
    con: duckdb.DuckDBPyConnection,
    values: list[str],
    filter_by: str,
    path_kwargs: dict,
    where: str | None = None,
) -> tuple[duckdb.DuckDBPyConnection, duckdb.DuckDBPyRelation]:
    """
    Cursor and relation over the polygons of some values in the GeoParquet.

    See `parquet_response` for the parameters; the caller closes the cursor.

    Raises
    ------
    HTTPException
        404 if there is no GeoParquet file, if it cannot be filtered by
        `filter_by` or if some values are not found; 400 if `where` is not
        allowed or not valid.
    """
    cursor = client.connect(con.cursor())
    try:
        relation = client.read_parquet(cursor, values, filter_by, **path_kwargs)
    except (OSError, ValueError) as e:
        cursor.close()
        raise HTTPException(404, str(e)) from None
    return cursor, filter_relation(cursor, relation, where)


def files_response(
    con: duckdb.DuckDBPyConnection,
    values: list[str],
    filter_by: str,
    path_kwargs: dict,
    headers: dict | None = None,
    download: bool = False,
    where: str | None = None,
) -> StreamingResponse | RedirectResponse:
    """
    GeoJSON of some values when there is no GeoParquet (e.g. 2022).

    One value: redirect to its GeoJSON file, unless it is downloaded (a
    redirect cannot name the file) or filtered with `where`. Several values,
    a download or a filter: the files are read and merged into one
    FeatureCollection.

    Parameters
    ----------
    con, values, filter_by :
        See `parquet_response`.
    path_kwargs : dict
        Arguments of `create_path_bucket`, except `filter_by`, `value`,
        `vectorfile_format` and `territory`.
    headers : dict, optional
        Headers of the response, when it is not a redirect.
    download : bool
        Whether the file is downloaded (see `geojson`).
    where : str, optional
        See `parquet_response`.

    Returns
    -------
    StreamingResponse or RedirectResponse

    Raises
    ------
    HTTPException
        404 if a file does not exist; 400 if `where` is not allowed or not
        valid.
    """
    try:
        urls = client.geojson_urls(
            values, filter_by, territory="metropole", **path_kwargs
        )
    except OSError as e:
        raise HTTPException(404, str(e)) from None
    if len(urls) == 1 and not download and not where:
        return RedirectResponse(urls[0], status_code=307)
    cursor = client.connect(con.cursor())
    try:
        relation = client.read_geojson(cursor, urls)
    except (OSError, duckdb.IOException, duckdb.HTTPException) as e:
        cursor.close()
        raise HTTPException(404, str(e)) from None
    relation = filter_relation(cursor, relation, where)
    return stream_geojson(cursor, relation, headers)


def geojson(
    request: Request,
    year: int,
    borders: str,
    filter_by: str,
    values: Annotated[list[str], Query(min_length=1)],
    crs: int = 4326,
    simplification: int | None = None,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    download: bool = False,
    where: str | None = None,
) -> StreamingResponse | RedirectResponse:
    """
    Polygons of `borders` for some values of `filter_by`, as GeoJSON.

    Same parameters as `carti_download` (e.g.
    ``/v1/geojson?year=2025&borders=COMMUNE&filter_by=DEPARTEMENT&values=75&values=92``).
    Read from the GeoParquet; for a year without GeoParquet (2022), see
    `files_response`. The file is named after the level and the year (e.g.
    DEP2026.geojson): shown by a browser, or downloaded with ``download=1``.
    Without `simplification`, 80 (50 for the GeoJSON files of 2022, which
    have no 80 version), as `carti_download`. `where` keeps only the
    polygons matching a DuckDB SQL expression, as in `carti_download`
    (e.g. ``where=POPULATION > 2000``); see `check_where` for what it may
    contain.
    """
    filter_by = filter_by.upper()
    path_kwargs = request_path_kwargs(
        year,
        borders,
        crs,
        DEFAULT_SIMPLIFICATION if simplification is None else simplification,
        path_within_bucket,
    )
    headers = content_disposition(file_name(borders, year, "geojson"), download)
    con = request.app.state.con
    if not parquet_available(filter_by, tuple(sorted(path_kwargs.items()))):
        if simplification is None:
            path_kwargs["simplification"] = GEOJSON_DEFAULT_SIMPLIFICATION
        return files_response(
            con, values, filter_by, path_kwargs, headers, download, where
        )
    return parquet_response(con, values, filter_by, path_kwargs, headers, where)


def geoparquet(
    request: Request,
    year: int,
    borders: str,
    filter_by: str,
    values: Annotated[list[str], Query(min_length=1)],
    crs: int = 4326,
    simplification: int | None = None,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    where: str | None = None,
) -> FileResponse:
    """
    Polygons of `borders` for some values of `filter_by`, as a GeoParquet
    file to download, named after the level and the year (e.g.
    DEP2026.parquet). Same parameters as `geojson` (`where` included); only
    for the years published as GeoParquet.
    """
    filter_by = filter_by.upper()
    path_kwargs = request_path_kwargs(
        year,
        borders,
        crs,
        DEFAULT_SIMPLIFICATION if simplification is None else simplification,
        path_within_bucket,
    )
    cursor, relation = read_parquet(
        request.app.state.con, values, filter_by, path_kwargs, where
    )
    # Written by DuckDB (GeoParquet metadata, CRS) to a temporary file,
    # deleted once sent
    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        relation.to_parquet(path)
    except WHERE_ERRORS as e:
        os.remove(path)
        raise HTTPException(400, str(e)) from None
    except Exception:
        os.remove(path)
        raise
    finally:
        cursor.close()
    return FileResponse(
        path,
        media_type=PARQUET_MEDIA_TYPE,
        filename=file_name(borders, year, "parquet"),
        background=BackgroundTask(os.remove, path),
    )


def request_path_kwargs(
    year: int, borders: str, crs: int, simplification: int, path_within_bucket: str
) -> dict:
    """Arguments of the client path functions for a request."""
    return {
        **DEFAULT_PATH_KWARGS,
        "path_within_bucket": check_path_within_bucket(path_within_bucket),
        "year": year,
        "borders": borders.upper(),
        "crs": crs,
        "simplification": simplification,
    }


def legacy_geojson(request: Request, path: str) -> StreamingResponse | RedirectResponse:
    """
    GeoJSON file of the S3 storage, built from the GeoParquet.

    Redirects to the file itself when there is no GeoParquet for the year.
    """
    match = LEGACY_PATH.fullmatch(path)
    if not match or match["bucket"] != BUCKET:
        raise HTTPException(404, f"Not a cartiflette GeoJSON path: {path}")
    path_kwargs = match.groupdict()
    filter_by = path_kwargs.pop("filter_by")
    value = path_kwargs.pop("value")
    check_path_within_bucket(path_kwargs["path_within_bucket"])
    if not parquet_available(filter_by, tuple(sorted(path_kwargs.items()))):
        return RedirectResponse(f"{client.ENDPOINT_URL}/{path}", status_code=307)
    return parquet_response(request.app.state.con, [value], filter_by, path_kwargs)


def health() -> dict:
    return {"status": "ok"}


def create_app(con: duckdb.DuckDBPyConnection | None = None) -> FastAPI:
    """
    The API application.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection, optional
        Connection to use. By default, one in-memory connection per
        application, created at startup: its cache of the remote files
        (``enable_external_file_cache``) is shared by all the requests.

    Returns
    -------
    FastAPI
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.con = client.connect(con or duckdb.connect())
        yield
        app.state.con.close()

    app = FastAPI(title="cartiflette", lifespan=lifespan)
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=1)
    app.get("/health")(health)
    app.get("/v1/geojson", response_model=None)(geojson)
    app.get("/v1/geoparquet", response_model=None)(geoparquet)
    app.get("/{path:path}", response_model=None)(legacy_geojson)
    return app
