import s3fs
import tempfile
import os

from dev import get_shapefile_ign

BUCKET = "lgaliana"
PATH_WITHIN_BUCKET = 'cartogether/shapefiles-test'

fs = s3fs.S3FileSystem(
  client_kwargs={'endpoint_url': 'https://minio.lab.sspcloud.fr'})


def dict_corresp_decoupage():
    corresp_decoupage_columns = {
        "region": 'INSEE_REG',
        "departement": "INSEE_DEP",
        "commune": "INSEE_COM"
        }
    return corresp_decoupage_columns


def keep_subset_geopandas(object, variable, values):
    if isinstance(values, (int, str, float)):
        return object.loc[object[variable] == values]
    else:
        return object.loc[object[variable].isin(values)]

def create_path_bucket(
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
    shapefile_format="geojson",
    decoupage="region",
    year="2022",
    value="28"
):
    write_path = f"{bucket}/{path_within_bucket}/{year}/{decoupage}/{value}/{shapefile_format}/raw.{shapefile_format}"
    if shapefile_format == "shp":
        write_path = write_path.rsplit("/", maxsplit=1)[0] + "/"
    return write_path


def write_shapefile_all_levels(
    object,
    level_var,
    shapefile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
):

    [write_shapefile_subset(
        object,
        shapefile_format=shapefile_format,
        decoupage=decoupage,
        year=year,
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        value=level) for level in object[level_var].unique()
    ]


def write_shapefile_subset(
  object, 
  value="28",
  shapefile_format="geojson",
  decoupage="region",
  year=2022,
  bucket=BUCKET,
  path_within_bucket=PATH_WITHIN_BUCKET,
) :

    corresp_decoupage_columns = dict_corresp_decoupage()
    
    format_standardized = {
        "geojson": 'geojson',
        "geopackage": "GPKG",
        "gpkg": "GPKG",
        "shp": "shp",
        "shapefile": "shp"
    }

    gpd_driver = {
        "geojson": "GeoJSON",
        "GPKG": "GPKG",
        "shp": None
    }

    format_write = format_standardized[shapefile_format.lower()]
    driver = gpd_driver[format_write]

    write_path = create_path_bucket(
        bucket=bucket,
        path_within_bucket=path_within_bucket,
        shapefile_format=format_write,
        decoupage=decoupage,
        year=year,
        value=value
    )
    
    if fs.exists(write_path):
        if format_write == "shp":
            dir_s3 = write_path
            [fs.rm(path_s3) for path_s3 in fs.ls(dir_s3)]
        else:
            fs.rm(write_path)  # single file
    
    object_subset = keep_subset_geopandas(
        object,
        corresp_decoupage_columns[decoupage],
        value)

    if format_write == "shp":
        write_shapefile_s3_shp(
            object=object,
            fs=fs,
            write_path=write_path,
            driver=driver)
    else:
        with fs.open(write_path, 'wb') as f:
            object_subset.to_file(
                f,
                driver=driver
            )   


def write_shapefile_s3_shp(
    object,
    fs,
    write_path,
    driver=None):

    print("When using shp format, we first need a local temporary save")

    tdir = tempfile.TemporaryDirectory()
    object.to_file(
        tdir.name + '/raw.shp',
        driver=driver)

    list_files_shp = os.listdir(tdir.name)

    [fs.put(f"{tdir.name}/{file_name}", f"{write_path}{file_name}") for file_name in list_files_shp]



def write_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2022,
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET):

    corresp_decoupage_columns = dict_corresp_decoupage()
    var_decoupage = corresp_decoupage_columns[decoupage]

    # IMPORT SHAPEFILES ------------------

    territories = {
        f: get_shapefile_ign(
            level=level,
            field=f) for f in [
                "metropole", "martinique",
                "reunion", "guadeloupe",
                "guyane"
                ]
        }

    # WRITE ALL
     
    [
        write_shapefile_all_levels(
            territory,
            var_decoupage) for territory in territories
    ]


def open_shapefile_from_s3(
    shapefile_format,
    decoupage,
    year, 
    value
):
    read_path = create_path_bucket(
        shapefile_format=shapefile_format,
        decoupage=decoupage,
        year=year,
        value=value)
    return fs.open(read_path, mode="r")


def write_shapefile_from_s3(
    filename,
    decoupage, year,
    value,
    shapefile_format="geojson"
):
    read_path = create_path_bucket(
        shapefile_format=shapefile_format,
        decoupage=decoupage,
        year=year,
        value=value)

    fs.download(
        read_path,
        filename)

    print(
        f"Requested file has been saved at location {filename}"
    )