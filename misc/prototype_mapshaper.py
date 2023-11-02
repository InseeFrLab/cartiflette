import s3fs
import os
import subprocess

from cartiflette.download.download import _download_sources
from cartiflette.utils import create_path_bucket

ENDPOINT_URL = "https://minio.lab.sspcloud.fr"

fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})



provider = "IGN"
source = "EXPRESS-COG-CARTO-TERRITOIRE",
year = 2022
provider = "IGN"
dataset_family = "ADMINEXPRESS"
source = "EXPRESS-COG-CARTO-TERRITOIRE"
territory = "metropole"
path_within_bucket = "test-download6"
crs = 4326
bucket = "projet-cartiflette"

dict_corresp = {"REGION": "INSEE_REG", "DEPARTEMENT": "INSEE_DEP"}

borders="COMMUNE" #tempdf['borders'].iloc[0]
format_output="topojson" #tempdf['format'].iloc[0]
niveau_agreg="DEPARTEMENT"#tempdf['filter_by'].iloc[0]
simplification = 0

# DOWNLOAD =========================

def upload_s3_raw(
    provider="IGN",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022,
    dataset_family="ADMINEXPRESS",
    territory="metropole",
    borders="COMMUNE",
    path_within_bucket="test-download6",
    crs=4326,
    bucket="projet-cartiflette"
    ):

    x = _download_sources(
        upload=True,
        providers=provider,
        dataset_families=dataset_family,
        sources=source,
        territories=territory,
        years=year,
        path_within_bucket=path_within_bucket
    )


    paths = create_path_bucket(
    {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "year": year,
        "borders": None,
        "crs": 2154,
        "filter_by": "origin",
        "value": "raw",
        "vectorfile_format": "shp",
        "provider": provider,
        "dataset_family": dataset_family,
        "source": source,
        "territory": territory,
        "filename": "COMMUNE.shp",
    }
    )

    rawpaths = x[provider][dataset_family][source][territory][year]['paths']

    if rawpaths is None:
        path_raw_s3 = create_path_bucket(
        {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "year": year,
            "borders": None,
            "crs": 2154,
            "filter_by": "origin",
            "value": "raw",
            "vectorfile_format": "shp",
            "provider": provider,
            "dataset_family": dataset_family,
            "source": source,
            "territory": territory,
            "filename": "COMMUNE.shp",
            "simplification": 0
        }
        )
    else:
        path_raw_s3 = rawpaths[borders][0]


    path_bucket = path_raw_s3.rsplit("/", maxsplit=1)[0]

    return path_bucket


path_bucket_new = upload_s3_raw()
path_bucket = upload_s3_raw()

def list_raw_files_level(fs, path_bucket, borders):
    list_raw_files = fs.ls(f"{path_bucket}")
    list_raw_files = [
        chemin for chemin in list_raw_files if chemin.rsplit("/", maxsplit=1)[-1].startswith(f'{borders}.')
        ]
    return list_raw_files


def download_files_from_list(fs, list_raw_files, local_dir = "temp"):
    for files in list_raw_files:
        fs.download(
            files,
            f"{local_dir}/{files.rsplit('/', maxsplit=1)[-1]}"
        )
    return local_dir


def prepare_local_directory_mapshaper(
    path_bucket,
    borders="COMMUNE",
    niveau_agreg="DEPARTEMENT",
    format_output="topojson",
    simplification=0,
    local_dir="temp",
    fs=fs,
    ):

    os.makedirs(local_dir, exist_ok=True)
    # Get all raw shapefiles from Minio
    list_raw_files = list_raw_files_level(fs, path_bucket, borders=borders)
    download_files_from_list(fs, list_raw_files)
    local_path_destination = f"{local_dir}/{niveau_agreg}/{format_output}/{simplification}"
    os.makedirs(
        local_path_destination,
        exist_ok=True
    )
    paths = {
        "path_origin": local_dir, "path_destination": local_path_destination
    }
    return paths


def mapshaperize_shapefiles(
    local_dir="temp",
    filename_initial="COMMUNE",
    extension_initial="shp", 
    format_output="topojson",
    niveau_agreg="DEPARTEMENT",
    provider="IGN",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022,
    dataset_family="ADMINEXPRESS",
    territory="metropole",
    crs=4326,
    simplification=0
):

    simplification_percent = simplification if simplification is not None else 0

    dict_corresp = {"REGION": "INSEE_REG", "DEPARTEMENT": "INSEE_DEP"}

    output_path = f"{local_dir}/{niveau_agreg}/{format_output}/{simplification}"

    subprocess.run(
        (
            f"mapshaper {local_dir}/{filename_initial}.{extension_initial} name='' -proj EPSG:{crs} "
            f"-simplify {simplification_percent}% "
            f"-each \"SOURCE='{provider}:{source[0]}'\" "
            f"-split {dict_corresp[niveau_agreg]} "
            f"-o {output_path} format={format_output} extension=\".{format_output}\" singles"
        ),
        shell=True
    )

    return output_path



bucket = bucket
path_within_bucket = path_within_bucket
borders = "COMMUNE"

local_directories = prepare_local_directory_mapshaper(path_bucket_new)
mapshaperize_shapefiles(filename_initial = borders)
local_directories = prepare_local_directory_mapshaper(path_bucket_new, niveau_agreg="REGION")
mapshaperize_shapefiles(filename_initial = borders, niveau_agreg = "REGION")



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

