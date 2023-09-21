import os
import logging
from dotenv import load_dotenv
import s3fs

from cartiflette.download import MasterScraper, Dataset


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


path = store_vectorfile_ign(
    source="EXPRESS-COG-TERRITOIRE", year=2022, territory="metropole"
)
path = store_vectorfile_ign(
    source="EXPRESS-COG-TERRITOIRE", year=2021, territory="reunion"
)
path = store_vectorfile_ign(
    source="EXPRESS-COG-TERRITOIRE", year=2020, territory="reunion"
)
path = store_vectorfile_ign(
    source="EXPRESS-COG-TERRITOIRE", year=2019, territory="reunion"
)

for c in [
    "metropole",
    "reunion",
    "guadeloupe",
    "martinique",
    "mayotte",
    "guyane",
]:
    path = store_vectorfile_ign(
        source="EXPRESS-COG-TERRITOIRE", year=2022, territory=c
    )
    print(os.listdir(path))

for c in [
    "metropole",
    "reunion",
    "guadeloupe",
    "martinique",
    "mayotte",
    "guyane",
]:
    path = store_vectorfile_ign(
        source="EXPRESS-COG-TERRITOIRE", year=2021, field=c
    )
    print(os.listdir(path))

for c in [
    "metropole",
    "reunion",
    "guadeloupe",
    "martinique",
    "mayotte",
    "guyane",
]:
    path = store_vectorfile_ign(
        source="EXPRESS-COG-TERRITOIRE", year=2020, field=c
    )
    print(os.listdir(path))

for c in [
    "metropole",
    "reunion",
    "guadeloupe",
    "martinique",
    "mayotte",
    "guyane",
]:
    path = store_vectorfile_ign(
        source="EXPRESS-COG-TERRITOIRE", year=2019, field=c
    )
    print(os.listdir(path))


path = store_vectorfile_ign(source="EXPRESS-COG", year=2022, field="metropole")
path = store_vectorfile_ign(source="EXPRESS-COG", year=2021, field="metropole")
path = store_vectorfile_ign(source="EXPRESS-COG", year=2020, field="reunion")
path = store_vectorfile_ign(source="EXPRESS-COG", year=2019, field="reunion")
