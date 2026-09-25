from __future__ import annotations

from cartiflette.constants import BUCKET, PATH_WITHIN_BUCKET

# User-facing format names -> format of the files stored by cartiflette
FORMATS = {
    "geojson": "geojson",
    "parquet": "parquet",
    "geoparquet": "parquet",
}


def standardize_format(vectorfile_format: str) -> str:
    try:
        return FORMATS[vectorfile_format.lower()]
    except KeyError:
        raise ValueError(
            f"Unsupported format {vectorfile_format!r}: cartiflette files are "
            "available as 'geojson' or 'parquet'"
        ) from None


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
    simplification: int | float | None = 0,
    filename: str = "raw",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
) -> str:
    """
    Path of a cartiflette file within the S3 storage.

    This must stay identical to cartiflette/paths.py in the pipeline: it is
    the only contract between the pipeline and the clients.
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
