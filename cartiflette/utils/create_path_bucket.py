"""Module for communication with Minio S3 Storage
"""

from typing import Optional, TypedDict, Union

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET


# CREATE STANDARDIZED PATHS ------------------------


class ConfigDict(TypedDict):
    bucket: Optional[str]
    path_within_bucket: Optional[str]
    provider: str
    dataset_family: str
    source: str
    year: str
    borders: str
    crs: Optional[Union[int, str]]
    filter_by: str
    value: str
    vectorfile_format: str
    territory: str
    simplification: Optional[Union[int, str]]
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
    dataset_family = config.get("dataset_family")
    source = config.get("source")
    year = config.get("year")
    borders = config.get("borders")
    crs = config.get("crs", 2154)
    filter_by = config.get("filter_by")
    value = config.get("value")
    vectorfile_format = config.get("vectorfile_format")
    territory = config.get("territory")
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
