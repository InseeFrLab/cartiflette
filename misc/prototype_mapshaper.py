import s3fs
import cartiflette.s3 as s3
import os
import subprocess

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})

croisement_decoupage_level = {
    ## structure -> niveau geo: [niveau decoupage macro],
    "REGION": ["FRANCE_ENTIERE"],
    "ARRONDISSEMENT_MUNICIPAL" : ['DEPARTEMENT'], 
    "COMMUNE_ARRONDISSEMENT": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "COMMUNE": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "DEPARTEMENT": ["REGION", "FRANCE_ENTIERE"]
}



#formats = ["geoparquet", "shp", "gpkg", "geojson"]
formats = ["topojson"]
#formats = ["geojson"]

#years = [y for y in range(2021, 2023)]
years = [2022]

#crs_list = [4326, 2154, "official"]
crs_list = [4326]

sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]


#tempdf = s3.crossproduct_parameters_production(
#        croisement_filter_by_borders=croisement_decoupage_level,
#        list_format=formats,
#        years=years,
#        crs_list=crs_list,
#        sources=sources,
#    )

dict_corresp = {"REGION": "INSEE_REG"}


bucket = "projet-cartiflette"
path_within_bucket = "diffusion/shapefiles-test2"
year=2022
provider="IGN"
source='EXPRESS-COG-TERRITOIRE'
field="metropole"

borders="COMMUNE" #tempdf['borders'].iloc[0]
format_output="geojson" #tempdf['format'].iloc[0]
niveau_agreg="REGION"#tempdf['filter_by'].iloc[0]


path_bucket = f"{bucket}/{path_within_bucket}/{year=}/raw/{provider=}/{source=}/{field=}"

list_raw_files = fs.ls(f"{path_bucket}")
list_raw_files = [
    chemin for chemin in list_raw_files if chemin.rsplit("/", maxsplit=1)[-1].startswith(f'{borders}.')
    ]

os.mkdir("temp")

for files in list_raw_files:
    fs.download(
        files,
        "temp/" +\
            files.rsplit("/", maxsplit=1)[-1]
    )


os.mkdir(niveau_agreg)


subprocess.run(
    f"mapshaper temp/{borders}.shp name='' -split {dict_corresp[niveau_agreg]} -o '{niveau_agreg}/' format={format_output}",
    shell=True
)





