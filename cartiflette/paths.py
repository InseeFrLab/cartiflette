"""
S3 path layout shared with the clients.

This must stay identical to ``python-package/cartiflette/cartiflette/utils.py``:
it is the only contract between the pipeline and the clients.
"""

from __future__ import annotations


def create_path_bucket(
    *,
    bucket: str,
    path_within_bucket: str,
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
) -> str:
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
    bucket: str,
    path_within_bucket: str,
    provider: str,
    dataset_family: str,
    source: str,
    year: int | str,
    borders: str,
    crs: int | str,
    geometry: str,
    simplification: float | None = 0,
    filename: str = "raw",
) -> str:
    """
    Path of a consolidated GeoParquet file: all the polygons of a level, to be
    filtered when read. `geometry` is FRANCE_ENTIERE (true positions) or
    FRANCE_ENTIERE_DROM_RAPPROCHES (DROM moved, Ile-de-France zoomed in).
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
