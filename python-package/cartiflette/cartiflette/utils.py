import typing
import logging

from cartiflette.constants import BUCKET, PATH_WITHIN_BUCKET

logger = logging.getLogger(__name__)


def dict_corresp_filter_by() -> dict:
    """Transforms explicit administrative borders into relevant column

    Returns:
        dict: Relevant column as well as initial
            user prompted administrative level
    """
    corresp_decoupage_columns = {
        "region": "INSEE_REG",
        "departement": "INSEE_DEP",
        "commune": "INSEE_COM",
        "commune_arrondissement": "INSEE_COM",
        "region_arrondissement": "INSEE_REG",
        "departement_arrondissement": "INSEE_DEP",
        "france_entiere": "territoire",
    }
    return corresp_decoupage_columns


def create_format_standardized() -> dict:
    """Transforms user-prompted format into geopandas format

    Returns:
        dict: Geopandas format as well as user-prompted
         format
    """
    format_standardized = {
        "geojson": "geojson",
        "geopackage": "GPKG",
        "gpkg": "GPKG",
        "shp": "shp",
        "shapefile": "shp",
        "geoparquet": "parquet",
        "parquet": "parquet",
        "topojson": "topojson",
    }
    return format_standardized


def create_format_driver() -> dict:
    """Transforms user-prompted format into Geopandas driver

    Returns:
        dict: Geopandas driver as well as user-prompted
         format
    """
    gpd_driver = {
        "geojson": "GeoJSON",
        "GPKG": "GPKG",
        "shp": None,
        "parquet": None,
        "topojson": None,
    }
    return gpd_driver


def standardize_inputs(vectorfile_format):
    corresp_filter_by_columns = dict_corresp_filter_by()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_write = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_write]

    return corresp_filter_by_columns, format_write, driver


class ConfigDict(typing.TypedDict):
    bucket: typing.Optional[str]
    path_within_bucket: typing.Optional[str]
    provider: str
    source: str
    vectorfile_format: str
    borders: str
    filter_by: str
    year: str
    crs: typing.Optional[int]
    value: str
    filename: typing.Optional[str]


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
