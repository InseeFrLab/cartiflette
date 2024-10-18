import subprocess
from cartiflette import carti_download
from cartiflette.pipeline_constants import COG_TERRITOIRE
from cartiflette.config import DATASETS_HIGH_RESOLUTION

ile_de_france = carti_download(
    values=["11"],
    crs=4326,
    borders="DEPARTEMENT",
    vectorfile_format="geojson",
    filter_by="REGION",
    source=COG_TERRITOIRE[DATASETS_HIGH_RESOLUTION],
    year=2022,
    provider="Cartiflette",
    dataset_family="production",
)

ile_de_france.to_file("idf.json")

subprocess.run(
    "mapshaper idf.json -o idf.topojson", check=False, shell=True, text=True
)
