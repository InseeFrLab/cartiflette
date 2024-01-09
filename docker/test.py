import subprocess
from cartiflette.public import download_from_cartiflette

ile_de_france = download_from_cartiflette(
      values = ["11"],
      crs = 4326,
      borders = "DEPARTEMENT",
      vectorfile_format="topojson",
      filter_by="REGION",
      source="EXPRESS-COG-CARTO-TERRITOIRE",
      year=2022,
      path_within_bucket = "test-download28")

ile_de_france.to_file("idf.json")

subprocess.run(
    "mapshaper idf.json -o idf.topojson",
    check=False,
    shell=True
)