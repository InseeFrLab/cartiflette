from __future__ import annotations

from cartiflette.constants import BUCKET, PATH_WITHIN_BUCKET

# User-facing format names -> format of the files stored by cartiflette
FORMATS = {
    "geojson": "geojson",
    "parquet": "parquet",
    "geoparquet": "parquet",
}


def standardize_format(vectorfile_format: str) -> str:
    """
    Format of the stored files matching a user-facing format name.

    Parameters
    ----------
    vectorfile_format : str
        "geojson", "parquet" or "geoparquet" (case insensitive).

    Returns
    -------
    str
        "geojson" or "parquet".

    Raises
    ------
    ValueError
        If the format is not available.

    Examples
    --------
    >>> standardize_format("GeoParquet")
    'parquet'
    """
    try:
        return FORMATS[vectorfile_format.lower()]
    except KeyError:
        raise ValueError(
            f"Unsupported format {vectorfile_format!r}: cartiflette files are "
            "available as 'geojson' or 'parquet'"
        ) from None


def value_candidates(filter_by: str, value: str | int) -> list[str]:
    """
    Values to try, in order, in the path of a file.

    Region codes below 10 (DROM) are accepted as 1, "1" or "01": files from
    2025 onwards use the official code ("01"), older files the unpadded one
    ("1").

    Parameters
    ----------
    filter_by : str
        Level used to select the polygons (e.g. "REGION").
    value : str or int
        Requested value.

    Returns
    -------
    list of str
        Spellings to try, the preferred one first.

    Examples
    --------
    >>> value_candidates("REGION", 1)
    ['01', '1']
    >>> value_candidates("DEPARTEMENT", "75")
    ['75']
    """
    value = str(value)
    if filter_by.upper() == "REGION" and value.isdigit() and int(value) < 10:
        return [f"{int(value):02d}", str(int(value))]
    return [value]


def create_path_bucket(
    *,
    provider: str,
    dataset_family: str,
    source: str,
    year: int | str,
    borders: str,
    crs: int | str,
    filter_by: str,
    value: str,
    vectorfile_format: str,
    territory: str,
    simplification: float | None = 0,
    filename: str = "raw",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
) -> str:
    """
    Path of a cartiflette file within the S3 storage.

    This must stay identical to cartiflette/paths.py in the pipeline: it is
    the only contract between the pipeline and the clients.

    Parameters
    ----------
    provider : str
        Provider of the data, "IGN".
    dataset_family : str
        Dataset family, "ADMINEXPRESS".
    source : str
        Source label, "EXPRESS-COG-CARTO-TERRITOIRE".
    year : int or str
        Vintage.
    borders : str
        Level of the polygons (e.g. "DEPARTEMENT").
    crs : int or str
        EPSG code.
    filter_by : str
        Level used to split the files (e.g. "REGION").
    value : str
        Value of `filter_by` (e.g. "11").
    vectorfile_format : str
        "geojson" or "parquet".
    territory : str
        "metropole" for every published file.
    simplification : float, optional
        Simplification level, 0 or 50.
    filename : str
        Name of the file without extension, "raw".
    bucket : str
        Bucket of the files.
    path_within_bucket : str
        Prefix within the bucket, "production" for the published files.

    Returns
    -------
    str
        Path "bucket/path_within_bucket/provider=.../raw.{format}", to append
        to the S3 endpoint URL.

    Examples
    --------
    >>> create_path_bucket(
    ...     provider="IGN", dataset_family="ADMINEXPRESS",
    ...     source="EXPRESS-COG-CARTO-TERRITOIRE", year=2025,
    ...     borders="DEPARTEMENT", crs=4326, filter_by="REGION", value="11",
    ...     vectorfile_format="parquet", territory="metropole",
    ...     simplification=50,
    ... )  # doctest: +ELLIPSIS
    'projet-cartiflette/production/provider=IGN/.../simplification=50/raw.parquet'
    """
    simplification = int(simplification or 0)
    return (
        f"{bucket}/{path_within_bucket}"
        f"/provider={provider}"
        f"/dataset_family={dataset_family}"
        f"/source={source}"
        f"/year={year}"
        f"/administrative_level={borders}"
        f"/crs={crs}"
        f"/{filter_by}={value}"
        f"/vectorfile_format={vectorfile_format}"
        f"/territory={territory}"
        f"/simplification={simplification}"
        f"/{filename}.{vectorfile_format}"
    )


def create_path_consolidated(
    *,
    provider: str,
    dataset_family: str,
    source: str,
    year: int | str,
    borders: str,
    crs: int | str,
    geometry: str,
    simplification: float | None = 0,
    filename: str = "raw",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
) -> str:
    """
    Path of a consolidated GeoParquet file within the S3 storage.

    A consolidated file holds all the polygons of a level, to be filtered
    when read. This must stay identical to cartiflette/paths.py in the
    pipeline.

    Parameters
    ----------
    provider, dataset_family, source, year, borders, crs, simplification,
    filename, bucket, path_within_bucket :
        See `create_path_bucket`.
    geometry : str
        "FRANCE_ENTIERE" (true positions) or "FRANCE_ENTIERE_DROM_RAPPROCHES"
        (DROM moved next to metropolitan France, Ile-de-France zoomed in).

    Returns
    -------
    str
        Path "bucket/path_within_bucket/provider=.../raw.parquet".

    Examples
    --------
    >>> create_path_consolidated(
    ...     provider="IGN", dataset_family="ADMINEXPRESS",
    ...     source="EXPRESS-COG-CARTO-TERRITOIRE", year=2025,
    ...     borders="COMMUNE", crs=4326, geometry="FRANCE_ENTIERE",
    ...     simplification=50,
    ... )  # doctest: +ELLIPSIS
    'projet-cartiflette/production/.../geometry=FRANCE_ENTIERE/vectorfile_format=parquet/simplification=50/raw.parquet'
    """
    simplification = int(simplification or 0)
    return (
        f"{bucket}/{path_within_bucket}"
        f"/provider={provider}"
        f"/dataset_family={dataset_family}"
        f"/source={source}"
        f"/year={year}"
        f"/administrative_level={borders}"
        f"/crs={crs}"
        f"/geometry={geometry}"
        f"/vectorfile_format=parquet"
        f"/simplification={simplification}"
        f"/{filename}.parquet"
    )
