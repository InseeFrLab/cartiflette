import os

os.chdir("cartogether")

import cartiflette.s3 as s3
from cartiflette.download import get_administrative_level_available_ign


import itertools
formats=["geoparquet", "shp", "gpkg", "geojson"]
decoupage=["region", "departement"]
level=["COMMUNE", "ARRONDISSEMENT"]
for format, decoup, lev in itertools.product(formats, decoupage, level):
    s3.write_vectorfile_s3_all(
        level=lev,
        vectorfile_format=format,
        decoupage=decoup,
        year=2022)


# OLD --------------

s3.write_vectorfile_s3_all(
        level="ARRONDISSEMENT_MUNICIPAL",
        vectorfile_format="geojson",
        decoupage="departement",
        year=2022)


obj = s3.download_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    values = ["28","11"])

get_administrative_level_available_ign()


s3.write_vectorfile_s3_all(
    level="ARRONDISSEMENT",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

obj2 = s3.download_vectorfile_s3_single(
    level="COMMUNE",
    vectorfile_format="gpkg",
    decoupage="region",
    year=2022)

obj3 = s3.download_vectorfile_s3_single(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2022)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

s3.write_vectorfile_s3_all(
    level="ARRONDISSEMENT",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2021)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="GPKG",
    decoupage="region",
    year=2019)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="parquet",
    decoupage="region",
    year=2019)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2020)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2022)