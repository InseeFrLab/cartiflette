import os
from dotenv import load_dotenv
import s3fs
import logging

# os.chdir("cartiflette")

import cartiflette.s3 as s3
from cartiflette.download import get_administrative_level_available_ign

# Configuration du logger
script_file = os.path.basename(__file__)
script_file, _ext = os.path.splitext(script_file)
kwargs_log = {
    "format": "%(levelname)s:%(asctime)s %(message)s",
    "datefmt": "%m/%d/%Y %H:%M",
    "level": logging.INFO,
}
file_handler = logging.FileHandler(
    f"log_{script_file}.log", mode="w", encoding="utf8"
)
console_handler = logging.StreamHandler()
kwargs_log.update({"handlers": [file_handler, console_handler]})
logging.basicConfig(**kwargs_log)

# Chargement des éventuels tokens SSPCloud
load_dotenv()

bucket = "projet-cartiflette"
path_within_bucket = "diffusion/shapefiles-test4"
endpoint_url = "https://minio.lab.sspcloud.fr"
base_cache_pattern = os.path.join("**", "*DONNEES_LIVRAISON*", "**")

kwargs = {}
for key in ["token", "secret", "key"]:
    try:
        kwargs[key] = os.environ[key]
    except KeyError:
        continue
fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint_url}, **kwargs)


# RECUPERATION SHAPEFILES IGN -----------------------------

sources = ["EXPRESS-COG-TERRITOIRE"]
territories = [
    "metropole",
    "reunion",
    "guadeloupe",
    "martinique",
    "mayotte",
    "guyane",
]
years = [2022, 2021]
providers = ["IGN"]


s3.duplicate_vectorfile_ign(
    sources=sources,
    territories=territories,
    years=years,
    providers=providers,
    path_within_bucket=path_within_bucket,
    base_cache_pattern=base_cache_pattern,
    fs=fs,
)


# PRODUCTION SHAPEFILES CARTIFLETTE ----------------------


# formats = ["geoparquet", "shp", "gpkg", "geojson"]
# formats = ["topojson"]
formats = ["geojson"]

# years = [y for y in range(2021, 2023)]
years = [2022]

# crs_list = [4326, 2154, "official"]
crs_list = [4326, "official"]

sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]

croisement_decoupage_level = {
    ## structure -> niveau geo: [niveau decoupage macro],
    "REGION": ["FRANCE_ENTIERE"],
    "ARRONDISSEMENT_MUNICIPAL": ["DEPARTEMENT"],
    "COMMUNE_ARRONDISSEMENT": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "COMMUNE": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "DEPARTEMENT": ["REGION", "FRANCE_ENTIERE"],
}

path_within_bucket = "diffusion/shapefiles-test2"
s3.production_cartiflette(
    croisement_decoupage_level,
    formats,
    years,
    crs_list,
    sources,
    path_within_bucket=path_within_bucket,
    fs=fs,
)

s3.write_cog_s3(
    year=2022,
    vectorfile_format="parquet",
    path_within_bucket=path_within_bucket,
    fs=fs,
)
s3.write_cog_s3(
    year=2021,
    vectorfile_format="parquet",
    path_within_bucket=path_within_bucket,
    fs=fs,
)

s3.list_produced_cartiflette(
    bucket=bucket,
    path_within_bucket=path_within_bucket,
    fs=fs,
)

# OLD --------------

s3.download_vectorfile_url_all(
    values="metropole",
    level="REGION",
    vectorfile_format="geojson",
    decoupage="france_entiere",
    year=2022,
)

s3.write_vectorfile_s3_all(
    level="ARRONDISSEMENT_MUNICIPAL",
    vectorfile_format="geojson",
    decoupage="departement",
    year=2022,
    fs=fs,
)


obj = s3.download_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    values=["28", "11"],
    fs=fs,
)

get_administrative_level_available_ign()


s3.write_vectorfile_s3_all(
    level="ARRONDISSEMENT",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    fs=fs,
)

obj2 = s3.download_vectorfile_s3_single(
    level="COMMUNE",
    vectorfile_format="gpkg",
    decoupage="region",
    year=2022,
    fs=fs,
)

obj3 = s3.download_vectorfile_s3_single(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2022,
    fs=fs,
)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    fs=fs,
)

s3.write_vectorfile_s3_all(
    level="ARRONDISSEMENT",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    fs=fs,
)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2021,
    fs=fs,
)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="GPKG",
    decoupage="region",
    year=2019,
    fs=fs,
)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="parquet",
    decoupage="region",
    year=2019,
    fs=fs,
)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2020,
    fs=fs,
)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2022,
    fs=fs,
)
