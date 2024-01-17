from cartiflette.download.download import _download_sources
from cartiflette.utils import create_path_bucket, official_epsg_codes
from cartiflette.config import PATH_WITHIN_BUCKET


def upload_s3_raw(
    provider="IGN",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022,
    dataset_family="ADMINEXPRESS",
    territory="metropole",
    borders="COMMUNE",
    path_within_bucket=PATH_WITHIN_BUCKET,
    vectorfile_format="shp",
    bucket="projet-cartiflette",
):
    """
    Uploads raw data to an S3 bucket and returns the path to the bucket.

    Parameters
    ----------
    provider : str, optional
        The provider of the data, by default "IGN".
    source : str, optional
        The data source, by default "EXPRESS-COG-CARTO-TERRITOIRE".
    year : int, optional
        The year of the data, by default 2022.
    dataset_family : str, optional
        The dataset family, by default "ADMINEXPRESS".
    territory : str, optional
        The territory of the data, by default "metropole".
    borders : str, optional
        The type of borders, by default "COMMUNE".
    path_within_bucket : str, optional
        The path within the S3 bucket, by default cartiflette.config.PATH_WITHIN_BUCKET.
    bucket : str, optional
        The S3 bucket name, by default "projet-cartiflette".

    Returns
    -------
    str
        The path to the S3 bucket where the raw data is uploaded.

    """

    x = _download_sources(
        upload=True,
        providers=provider,
        dataset_families=dataset_family,
        sources=source,
        territories=territory,
        years=year,
        path_within_bucket=path_within_bucket,
    )

    rawpaths = x[provider][dataset_family][source][territory][year]["paths"]

    if rawpaths is None:
        path_raw_s3 = create_path_bucket(
            {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "borders": None,
                "crs": official_epsg_codes()[territory],
                "filter_by": "origin",
                "value": "raw",
                "vectorfile_format": vectorfile_format,
                "provider": provider,
                "dataset_family": dataset_family,
                "source": source,
                "territory": territory,
                "filename": "COMMUNE.shp",
                "simplification": 0,
            }
        )
    else:
        path_raw_s3 = rawpaths[borders][0]

    path_bucket = path_raw_s3.rsplit("/", maxsplit=1)[0]

    return path_bucket
