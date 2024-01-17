import warnings
from cartiflette import carti_download
from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET


def download_vectorfile_url_all(*args, **kwargs):

    warnings.warn(
        "download_vectorfile_url_all is deprecated and will be removed in a future version. "
        "Please use carti_download instead (from cartiflette import carti_download).",
        DeprecationWarning,
        stacklevel=2,
    )

    values = kwargs.get("values", [])
    borders = kwargs.get("borders", "COMMUNE")
    filter_by = kwargs.get("filter_by", "region")
    territory = kwargs.get("territory", "metropole")
    vectorfile_format = kwargs.get("vectorfile_format", "geojson")
    year = kwargs.get("year", None)
    crs = kwargs.get("crs", 2154)
    simplification = kwargs.get("simplification", None)

    # Assuming bucket and path_within_bucket are constants in your 'cartiflette' module
    bucket = kwargs.get("bucket", BUCKET)
    path_within_bucket = kwargs.get("path_within_bucket", PATH_WITHIN_BUCKET)

    provider = kwargs.get("provider", "IGN")
    dataset_family = kwargs.get("dataset_family", "ADMINEXPRESS")
    source = kwargs.get("source", "EXPRESS-COG-TERRITOIRE")

    # Call the new function
    return carti_download(
        values=values,
        borders=borders,
        filter_by=filter_by,
        territory=territory,
        vectorfile_format=vectorfile_format,
        year=year,
        crs=crs,
        simplification=simplification,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        provider=provider,
        dataset_family=dataset_family,
        source=source,
    )
