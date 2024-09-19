"""Module for communication with Minio S3 Storage
"""

from typing import Optional, TypedDict

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET


# CREATE STANDARDIZED PATHS ------------------------


class ConfigDict(TypedDict):
    bucket: Optional[str]
    path_within_bucket: Optional[str]
    provider: str
    source: str
    vectorfile_format: str
    borders: str
    filter_by: str
    year: str
    crs: Optional[int]
    value: str
    filename: Optional[str]


def create_path_bucket(config: ConfigDict) -> str:
    """
    This function creates a file path for a vector file within a specified
    bucket.

    Parameters
    ----------
    config : ConfigDict
        A dictionary containing vector file parameters.

    Returns
    -------
    str
       The complete file path for the vector file that will be used to read
       or write when interacting with S3 storage.

    """

    bucket = config.get("bucket", BUCKET)
    path_within_bucket = config.get("path_within_bucket", PATH_WITHIN_BUCKET)

    provider = config.get("provider")
    source = config.get("source")

    vectorfile_format = config.get("vectorfile_format")
    borders = config.get("borders")
    dataset_family = config.get("dataset_family")
    territory = config.get("territory")
    filter_by = config.get("filter_by")
    year = config.get("year")
    value = config.get("value")
    crs = config.get("crs", 2154)
    simplification = config.get("simplification", 0)
    filename = config.get("filename")

    if simplification is None:
        simplification = 0

    simplification = int(simplification)

    # Un hack pour modifier la valeur si jamais le pattern du filename n'est pas raw.{vectorfile_format}
    if filename == "value":
        filename = value

    write_path = (
        f"{bucket}/{path_within_bucket}"
        f"/{provider=}"
        f"/{dataset_family=}"
        f"/{source=}"
        f"/{year=}"
        f"/administrative_level={borders}"
        f"/{crs=}"
        f"/{filter_by}={value}"
        f"/{vectorfile_format=}"
        f"/{territory=}"
        f"/{simplification=}"
    ).replace("'", "")

    if filename:
        write_path += f"/{filename}.{vectorfile_format}"
    elif vectorfile_format == "shp":
        write_path += "/"
    else:
        write_path += f"/raw.{vectorfile_format}"

    return write_path


# if __name__ == "__main__":
#     ret = create_path_bucket(
#         {
#             "bucket": BUCKET,
#             "path_within_bucket": PATH_WITHIN_BUCKET,
#             "provider": "IGN",
#             "source": "ADMINEXPRESS",
#             "vectorfile_format": "shp",
#             "borders": "COMMUNE",
#             "filter_by": None,
#             "year": 2022,
#             "value": None,
#             "crs": "2154",
#         }
#     )

#     print(ret)
