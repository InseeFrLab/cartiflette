import s3fs
import cartiflette.s3 as s3
import os
import subprocess

ENDPOINT_URL = "https://minio.lab.sspcloud.fr"


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

dict_corresp = {"REGION": "INSEE_REG", "DEPARTEMENT": "INSEE_DEP"}


bucket = "projet-cartiflette"
path_within_bucket = "diffusion/shapefiles-test2"
year=2022
provider="IGN"
source='EXPRESS-COG-TERRITOIRE'
field="metropole"

borders="COMMUNE" #tempdf['borders'].iloc[0]
format_output="topojson" #tempdf['format'].iloc[0]
niveau_agreg="DEPARTEMENT"#tempdf['filter_by'].iloc[0]


path_bucket = f"{bucket}/{path_within_bucket}/{year=}/raw/{provider=}/{source=}/{field=}"


def list_raw_files_level(fs, path_bucket, borders):
    list_raw_files = fs.ls(f"{path_bucket}")
    list_raw_files = [
        chemin for chemin in list_raw_files if chemin.rsplit("/", maxsplit=1)[-1].startswith(f'{borders}.')
        ]
    return list_raw_files


def download_files_from_list(fs, list_raw_files):
    for files in list_raw_files:
        fs.download(
            files,
            "temp/" +\
                files.rsplit("/", maxsplit=1)[-1]
        )

os.mkdir("temp")
list_raw_files = list_raw_files_level(fs, path_bucket, borders=borders)
download_files_from_list(fs, list_raw_files)

os.mkdir(niveau_agreg)


subprocess.run(
    f"mapshaper temp/{borders}.shp name='' -proj wgs84 \
        -each \"SOURCE='{provider}:{source}'\"\
        -split {dict_corresp[niveau_agreg]} \
        -o '{niveau_agreg}/' format={format_output} extension=\".{format_output}\" singles",
    shell=True
)

# topojson & niveau communal
format_output="topojson"

subprocess.run(
    f"mapshaper temp/{borders}.shp name='' -proj wgs84 \
        -each \"SOURCE='{provider}:{source}'\"\
        -split {dict_corresp[niveau_agreg]} \
        -o '{niveau_agreg}/' format={format_output} extension=\".{format_output}\" singles",
    shell=True
)


# niveau commune_arrondissement
borders="ARRONDISSEMENT_MUNICIPAL"
list_raw_files = list_raw_files_level(fs, path_bucket, borders=borders)
download_files_from_list(fs, list_raw_files)


subprocess.run(
    f"mapshaper temp/COMMUNE.shp \
        -proj wgs84 \
        -filter '\"69123,13055,75056\".indexOf(INSEE_COM) > -1' invert \
        -each \"INSEE_COG=INSEE_ARM\" \
        -o format={format_output} extension=\".{format_output}\"",
    shell=True
)

subprocess.run(
    f"mapshaper temp/ARRONDISSEMENT_MUNICIPAL.shp \
        -proj wgs84 \
        -rename-fields INSEE_COG=INSEE_ARM \
        -each '\
                INSEE_DEP=INSEE_COG.substr(0,2), \
                STATUT=\"Arrondissement municipal\"\
            ' \
        -o format={format_output} extension=\".{format_output}\"",
    shell=True
)


subprocess.run(
    f"mapshaper COMMUNE.topojson ARRONDISSEMENT_MUNICIPAL.topojson combine-files \
        -proj wgs84 \
        -merge-layers target=COMMUNE,ARRONDISSEMENT_MUNICIPAL force \
        -rename-layers COMMUNE_ARRONDISSEMENT \
        -o format={format_output} extension=\".{format_output}\"",
    shell=True
)

# niveau departemental & niveaux supérieurs
borders = "DEPARTEMENT"
list_raw_files = list_raw_files_level(fs, path_bucket, borders=borders)
download_files_from_list(fs, list_raw_files)


subprocess.run(
    f"mapshaper temp/COMMUNE.shp \
        -proj wgs84 \
        -dissolve INSEE_DEP name=DEPARTEMENTS copy-fields=STATE_NAME,INSEE_REG sum-fields=POPULATION + \
        -dissolve INSEE_REG name=REGIONS copy-fields=STATE_NAME sum-fields=POPULATION + \
        -dissolve + name=france \
        -o out.topojson",
    shell=True
)

