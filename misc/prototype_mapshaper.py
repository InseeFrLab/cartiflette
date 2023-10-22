import s3fs
import os
import subprocess

from cartiflette.download.download import _download_sources
from cartiflette.utils import create_path_bucket

ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})



provider = "IGN"
source = "EXPRESS-COG-CARTO-TERRITOIRE",
dict_corresp = {"REGION": "INSEE_REG", "DEPARTEMENT": "INSEE_DEP"}
year = 2022
provider = "IGN"
dataset_family = "ADMINEXPRESS"
source = "EXPRESS-COG-CARTO-TERRITOIRE"
territory = "metropole"
path_within_bucket = "test-download5"
crs = 4326
bucket = "projet-cartiflette"

borders="COMMUNE" #tempdf['borders'].iloc[0]
format_output="topojson" #tempdf['format'].iloc[0]
niveau_agreg="DEPARTEMENT"#tempdf['filter_by'].iloc[0]
simplification = 0

# DOWNLOAD =========================
  
x = _download_sources(
    upload = True,
    providers = provider,
    dataset_families = dataset_family,
    sources = source,
    territories = territory,
    years = year,
    path_within_bucket = path_within_bucket
)


# path_manual = create_path_bucket(
#  {
#      "bucket": bucket,
#      "path_within_bucket": path_within_bucket,
#      "year": year,
#      "borders": None,
#      "crs": 2154,
#      "filter_by": "origin",
#      "value": "raw",
#      "vectorfile_format": "shp",
#      "provider": provider,
#      "dataset_family": dataset_family,
#      "source": source,
#      "territory": territory,
#      "filename": "COMMUNE.shp",
#  }
# )

path = x['IGN']['ADMINEXPRESS']['EXPRESS-COG-CARTO-TERRITOIRE']['metropole'][2022]['paths']['COMMUNE'][0]
path_bucket = path.rsplit("/", maxsplit=1)[0]


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

os.makedirs(f"{niveau_agreg}/{format_output}/", exist_ok=True)

simplification_percent = simplification if simplification is not None else 0

subprocess.run(
    (
        f"mapshaper temp/{borders}.shp name='' -proj EPSG:{crs} "
        f"-simplify {simplification_percent}% "
        f"-each \"SOURCE='{provider}:{source[0]}'\" "
        f"-split {dict_corresp[niveau_agreg]} "
        f"-o {niveau_agreg}/{format_output}/ format={format_output} extension=\".{format_output}\" singles"
    ),
    shell=True
)

bucket = bucket
path_within_bucket = path_within_bucket

for values in os.listdir(f"{niveau_agreg}/{format_output}"):
    path_s3 = create_path_bucket(
            {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "borders": borders,
                "crs": crs,
                "filter_by": niveau_agreg,
                "value": values.replace(f".{format_output}", ""),
                "vectorfile_format": format_output,
                "provider": provider,
                "dataset_family": dataset_family,
                "source": source,
                "territory": territory,
                "simplification": simplification
            })
    fs.put(f"{niveau_agreg}/{format_output}/{values}", path_s3, recursive=True)


# OLD

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

# dissolve & export
subprocess.run(
    f"mapshaper temp/COMMUNE.shp \
        -proj wgs84 \
        -dissolve INSEE_DEP name=DEPARTEMENTS copy-fields=STATE_NAME,INSEE_REG sum-fields=POPULATION + \
        -dissolve INSEE_REG name=REGIONS copy-fields=STATE_NAME sum-fields=POPULATION + \
        -dissolve + name=france \
        -o out.topojson target=*",
    shell=True
)

