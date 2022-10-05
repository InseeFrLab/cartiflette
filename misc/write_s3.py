import os

os.chdir("cartogether")

import cartiflette.s3 as s3
from cartiflette.download import get_administrative_level_available_ign

obj = s3.download_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2022,
    values = ["28","11"])

get_administrative_level_available_ign()


s3.write_shapefile_s3_all(
    level="ARRONDISSEMENT",
    shapefile_format="geojson",
    decoupage="region",
    year=2022)

obj2 = s3.download_shapefile_s3_single(
    level="COMMUNE",
    shapefile_format="gpkg",
    decoupage="region",
    year=2022)

obj3 = s3.download_shapefile_s3_single(
    level="COMMUNE",
    shapefile_format="shp",
    decoupage="region",
    year=2022)

s3.write_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2022)

s3.write_shapefile_s3_all(
    level="ARRONDISSEMENT",
    shapefile_format="geojson",
    decoupage="region",
    year=2022)


s3.write_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2021)

s3.write_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="GPKG",
    decoupage="region",
    year=2019)

s3.write_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="parquet",
    decoupage="region",
    year=2019)


s3.write_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="shp",
    decoupage="region",
    year=2020)


s3.write_shapefile_s3_all(
    level="COMMUNE",
    shapefile_format="shp",
    decoupage="region",
    year=2022)