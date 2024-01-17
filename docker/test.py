import subprocess
from cartiflette import carti_download

ile_de_france = carti_download(
    values=["11"],
    crs=4326,
    borders="DEPARTEMENT",
    vectorfile_format="geojson",
    filter_by="REGION",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022,
)

ile_de_france.to_file("idf.json")

subprocess.run("mapshaper idf.json -o idf.topojson", check=False, shell=True)
