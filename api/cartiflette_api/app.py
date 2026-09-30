"""
GeoJSON API over the consolidated GeoParquet files.

The pipeline publishes one GeoParquet file per level; this API reads it with
the cartiflette client (DuckDB only downloads the row groups of the requested
values) and serializes the polygons as GeoJSON in SQL, streamed feature by
feature. It removes the need to publish one GeoJSON file per value.

Two routes:

- ``/v1/geojson``: query parameters, several values in one response.
- ``/{legacy GeoJSON path}``: the path of a GeoJSON file in the S3 storage
  (``projet-cartiflette/production/provider=IGN/.../raw.geojson``), so that the
  clients reading those files only change the host. When there is no
  GeoParquet for the year (2022), it redirects to the file itself.

Run with ``uvicorn --factory cartiflette_api.app:create_app``.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Annotated

import duckdb
from cartiflette import client
from cartiflette.constants import BUCKET, PATH_WITHIN_BUCKET
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse

MEDIA_TYPE = "application/geo+json"
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
# Prefix within the bucket: "production" or e.g. "test/v0.2.0", no ".."
PATH_WITHIN_BUCKET_PATTERN = re.compile(r"[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*")
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


def feature_collection(relation: duckdb.DuckDBPyRelation) -> Iterator[str]:
    """
    GeoJSON FeatureCollection of a relation, in chunks.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
        Relation with a `geometry` column in EPSG:4326 (RFC 7946).

    Yields
    ------
    str
        Pieces of the FeatureCollection, to concatenate.
    """
    features = relation.query("rel", features_sql(relation))
    yield '{"type":"FeatureCollection","features":['
    separator = ""
    while rows := features.fetchmany(BATCH_SIZE):
        yield separator + ",".join(row[0] for row in rows)
        separator = ","
    yield "]}"


def check_path_within_bucket(path_within_bucket: str) -> str:
    if not PATH_WITHIN_BUCKET_PATTERN.fullmatch(path_within_bucket) or (
        ".." in path_within_bucket.split("/")
    ):
        raise HTTPException(400, f"Invalid path_within_bucket {path_within_bucket!r}")
    return path_within_bucket


@lru_cache(maxsize=1024)
def parquet_available(filter_by: str, path_kwargs: tuple) -> bool:
    """`client.parquet_available`, cached: a HEAD request before 2025."""
    return client.parquet_available(filter_by, **dict(path_kwargs))


def geojson_response(
    con: duckdb.DuckDBPyConnection,
    values: list[str],
    filter_by: str,
    path_kwargs: dict,
) -> StreamingResponse:
    """
    Streamed GeoJSON of the polygons of some values.

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

    Returns
    -------
    StreamingResponse

    Raises
    ------
    HTTPException
        404 if there is no GeoParquet file, if it cannot be filtered by
        `filter_by` or if some values are not found.
    """
    cursor = client.connect(con.cursor())
    try:
        relation = client.read_parquet(cursor, values, filter_by, **path_kwargs)
    except (OSError, ValueError) as e:
        cursor.close()
        raise HTTPException(404, str(e)) from None

    def chunks() -> Iterator[str]:
        try:
            yield from feature_collection(relation)
        finally:
            cursor.close()

    return StreamingResponse(chunks(), media_type=MEDIA_TYPE)


def geojson(
    request: Request,
    year: int,
    borders: str,
    filter_by: str,
    values: Annotated[list[str], Query(min_length=1)],
    crs: int = 4326,
    simplification: int = 0,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
) -> StreamingResponse:
    """
    Polygons of `borders` for some values of `filter_by`, as GeoJSON.

    Same parameters as `carti_download` (e.g.
    ``/v1/geojson?year=2025&borders=COMMUNE&filter_by=DEPARTEMENT&values=75&values=92``).
    """
    path_kwargs = {
        **DEFAULT_PATH_KWARGS,
        "path_within_bucket": check_path_within_bucket(path_within_bucket),
        "year": year,
        "borders": borders.upper(),
        "crs": crs,
        "simplification": simplification,
    }
    return geojson_response(request.app.state.con, values, filter_by, path_kwargs)


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
    return geojson_response(request.app.state.con, [value], filter_by, path_kwargs)


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
    app.get("/v1/geojson", response_class=StreamingResponse)(geojson)
    app.get("/{path:path}", response_model=None)(legacy_geojson)
    return app
