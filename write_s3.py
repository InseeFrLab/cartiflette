import os

os.chdir("cartogether")

import s3


s3.download_shapefile_s3_single(
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2022)

s3.write_shapefile_s3_all(
    level="COMMUNE",
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