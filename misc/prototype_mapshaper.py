from cartiflette.s3 import upload_s3_raw
from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.pipeline import mapshaperize_split_from_s3, mapshaperize_merge_split_from_s3
from cartiflette.download.download import _download_sources

path_within_bucket = "test-download20"


# DATA RETRIEVING STEP =========================

import os
import pandas as pd
from cartiflette import FS
fs = FS

os.makedirs("temp", exist_ok=True)

# IGN DATASET
path_bucket_adminexpress = upload_s3_raw(
    path_within_bucket=path_within_bucket,
    year=2022
)

path_bucket_cog_commune = upload_s3_raw(
    provider='Insee',
    dataset_family='COG',
    source="COMMUNE",
    territory="france_entiere",
    borders="DATASET_INSEE_COG_COMMUNE_FRANCE_ENTIERE_2022",
    year=2022,
    crs=None,
    vectorfile_format="csv",
    path_within_bucket=path_within_bucket
    )

# DEPARTEMENT (FOR COMMON NAMES)
path_bucket_cog_departement = upload_s3_raw(
    provider='Insee',
    dataset_family='COG',
    source="DEPARTEMENT",
    territory="france_entiere",
    borders="DATASET_INSEE_COG_DEPARTEMENT_FRANCE_ENTIERE_2022",
    year=2022,
    crs=None,
    vectorfile_format="csv",
    path_within_bucket=path_within_bucket
    )

# REGIONS (FOR COMMON NAMES)
path_bucket_cog_region = upload_s3_raw(
    provider='Insee',
    dataset_family='COG',
    source="REGION",
    territory="france_entiere",
    borders="DATASET_INSEE_COG_REGION_FRANCE_ENTIERE_2022",
    year=2022,
    crs=None,
    vectorfile_format="csv",
    path_within_bucket=path_within_bucket
    )

# TABLE PASSAGE COMMUNES, DEP, REGIONS
path_bucket_tagc_appartenance = upload_s3_raw(
    provider='Insee',
    dataset_family='TAGC',
    source="APPARTENANCE",
    territory="france_entiere",
    borders="table-appartenance-geo-communes-22",
    year=2022,
    crs=None,
    vectorfile_format="xlsx",
    path_within_bucket=path_within_bucket
    )

path_bucket_tagc_passage = upload_s3_raw(
    provider='Insee',
    dataset_family='TAGC',
    source="PASSAGE_ANNUEL",
    territory="france_entiere",
    borders="table_passage_annuelle_2023",
    year=2023,
    crs=None,
    vectorfile_format="xlsx",
    path_within_bucket=path_within_bucket
    )


# PUTTING ALL METADATA TOGETHER
path_tagc = fs.ls(path_bucket_tagc_appartenance)[0]
path_bucket_cog_departement = fs.ls(path_bucket_cog_departement)[0]
path_bucket_cog_region = fs.ls(path_bucket_cog_region)[0]


with fs.open(path_tagc, mode = "rb") as remote_file:
    tagc = pd.read_excel(remote_file, skiprows=5, dtype_backend="pyarrow")

with fs.open(path_bucket_cog_departement, mode = "rb") as remote_file:
    cog_dep = pd.read_csv(remote_file, dtype_backend="pyarrow")

with fs.open(path_bucket_cog_region, mode = "rb") as remote_file:
    cog_region = pd.read_csv(remote_file, dtype_backend="pyarrow")


cog_metadata = cog_dep.loc[:, ['DEP','REG', 'LIBELLE']].merge(
    cog_region.loc[:, ['REG', 'LIBELLE']],
    on = "REG", suffixes = ['_DEPARTEMENT','_REGION']
).drop(columns = ['REG'])


tagc_metadata = tagc.merge(
    cog_metadata
)

tagc_metadata.drop(columns = ["LIBGEO"]).to_csv("temp/tagc.csv")



# TEST MAPSHAPERIZE

mapshaperize_split_from_s3(
    path_bucket_adminexpress,
    {
        'path_within_bucket': path_within_bucket,
        "borders": "COMMUNE",
        "filter_by": "REGION",
        "simplification": 50
    }
)

mapshaperize_merge_split_from_s3(
    path_bucket=path_bucket_adminexpress,
    config={
        'path_within_bucket': path_within_bucket,
        "simplification": 50,
        "filter_by": "DEPARTEMENT"
    }
)

mapshaperize_merge_split_from_s3(
    path_bucket=path_bucket_adminexpress,
    config={
        'path_within_bucket': path_within_bucket,
        "simplification": 50,
        "filter_by": "REGION"
    }
)


croisement_decoupage_level = {
    ## structure -> niveau geo: [niveau decoupage macro],
    # "REGION": ["FRANCE_ENTIERE"],
    #"ARRONDISSEMENT_MUNICIPAL" : ['DEPARTEMENT'], 
    #"COMMUNE_ARRONDISSEMENT": ["DEPARTEMENT", "REGION"],# "FRANCE_ENTIERE"],
    "COMMUNE": ["DEPARTEMENT", "REGION"],# "FRANCE_ENTIERE"],
    "DEPARTEMENT": ["REGION"]#, "FRANCE_ENTIERE"]
}



#formats = ["geoparquet", "shp", "gpkg", "geojson"]
#formats = ["topojson", "geojson"]
formats = ["geojson"]

#years = [y for y in range(2021, 2023)]
years = [2022]

#crs_list = [4326, 2154, "official"]
#crs_list = [4326, 2154]
crs_list = [4326]

sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]


tempdf = crossproduct_parameters_production(
        croisement_filter_by_borders=croisement_decoupage_level,
        list_format=formats,
        years=years,
        crs_list=crs_list,
        sources=sources,
        simplifications=[0, 50]
    )


for index, row in tempdf.iterrows():
    print(row)
    mapshaperize_split_from_s3(
        path_bucket,
        {
            **{'path_within_bucket': path_within_bucket},
            **row.to_dict()
        }
    )


# niveau commune_arrondissement

tempdf_arr = tempdf.loc[tempdf['borders'] == "COMMUNE"].copy()
tempdf_arr = tempdf_arr.drop(columns = ['borders'])

for index, row in tempdf_arr.iterrows():
    print(row)
    mapshaperize_merge_split_from_s3(
        path_bucket,
        {
            **{'path_within_bucket': path_within_bucket},
            **row.to_dict()
        }
    )



_download_sources(
    providers=['Insee'],
    dataset_families=['COG'],
    sources=["COMMUNE"],
    territories=["france_entiere"],
    years = [2022]
)

_download_sources(
    providers=['Insee'],
    dataset_families=['BV'],
    sources=["FondsDeCarte_BV_2022"],
    territories=["france_entiere"],
    years = [2022]
)


# old

from cartiflette.config import FS
from cartiflette.pipeline.prepare_mapshaper import prepare_local_directory_mapshaper
from cartiflette.mapshaper import mapshaperize_split_merge

local_dir = "temp/"

format_intermediate = "geojson"
local_directories = prepare_local_directory_mapshaper(
        path_bucket,
        borders="COMMUNE",
        niveau_agreg="DEPARTEMENT",
        format_output="topojson",
        simplification=0,
        local_dir=local_dir,
        fs=FS
)
local_directories = prepare_local_directory_mapshaper(
        path_bucket,
        borders="ARRONDISSEMENT_MUNICIPAL",
        niveau_agreg="DEPARTEMENT",
        format_output="topojson",
        simplification=0,
        local_dir=local_dir,
        fs=FS
)










# A intégrer

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



# old

from cartiflette.config import ENDPOINT_URL
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




bucket = "projet-cartiflette"
#path_within_bucket = "shapefiles-test2"
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


