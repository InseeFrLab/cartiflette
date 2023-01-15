import os

os.chdir("cartiflette")

import itertools
import cartiflette.s3 as s3
from cartiflette.download import get_administrative_level_available_ign

#formats = ["geoparquet", "shp", "gpkg", "geojson"]
formats = ["topojson"]

#years = [y for y in range(2021, 2023)]
years = [2021, 2022]

#crs_list = [4326, 2154, "official"]
crs_list = [4326, "official"]

sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]

croisement_decoupage_level = {
    ## structure -> niveau geo: [niveau decoupage macro],
    "REGION": ["FRANCE_ENTIERE"],
    "COMMUNE_ARRONDISSEMENT": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "COMMUNE": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "DEPARTEMENT": ["REGION", "FRANCE_ENTIERE"]
}


s3.production_cartiflette(
    croisement_decoupage_level,
    formats,
    years,
    crs_list,
    sources
)



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