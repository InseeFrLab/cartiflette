from cartiflette.s3 import upload_s3_raw
from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.pipeline import mapshaperize_split_from_s3

# DOWNLOAD =========================

path_within_bucket = "test-download9"

path_bucket = upload_s3_raw(path_within_bucket=path_within_bucket)

mapshaperize_split_from_s3(
    path_bucket,
    {
        'path_within_bucket': path_within_bucket,
        "borders": "COMMUNE",
        "filter_by": "REGION",
        "simplification": 50
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
formats = ["topojson", "geojson"]

#years = [y for y in range(2021, 2023)]
years = [2022]

#crs_list = [4326, 2154, "official"]
crs_list = [4326, 2154]

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

from cartiflette.config import FS
from cartiflette.pipeline.prepare_mapshaper import prepare_local_directory_mapshaper


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

simplification_percent = simplification if simplification is not None else 0

subprocess.run(
    (
    f"mapshaper {local_dir}/COMMUNE.{extension_initial} name='COMMUNE' "
    f"-proj EPSG:{crs} "
    f"-filter '\"69123,13055,75056\".indexOf(INSEE_COM) > -1' invert "
    f"-each \"INSEE_COG=INSEE_COM\" "
    f"-o {output_path}/communes_simples.{format_intermediate} format={format_intermediate} extension=\".{format_intermediate}\" singles"
    ),
    shell=True
)


subprocess.run(
    (
    f"mapshaper {local_dir}/ARRONDISSEMENT_MUNICIPAL.{extension_initial} name='ARRONDISSEMENT_MUNICIPAL' "
    f"-proj EPSG:{crs} "
    f"-rename-fields INSEE_COG=INSEE_ARM "
    f"-each 'INSEE_DEP=INSEE_COG.substr(0,2), STATUT=\"Arrondissement municipal\" ' "
    f"-o {output_path}/arrondissements.{format_intermediate} format={format_intermediate} extension=\".{format_intermediate}\""
    ),
    shell=True
)


subprocess.run(
    (
    f"mapshaper {output_path}/communes_simples.{format_intermediate} {output_path}/arrondissements.{format_intermediate} snap combine-files "
    f"-proj EPSG:{crs} "
    f"-rename-layers COMMUNE,ARRONDISSEMENT_MUNICIPAL "
    f"-merge-layers target=COMMUNE,ARRONDISSEMENT_MUNICIPAL force "
    f"-rename-layers COMMUNE_ARRONDISSEMENT "
    f"-o {output_path}/raw.{format_intermediate} format={format_intermediate} extension=\".{format_intermediate}\""
    ),
    shell=True
)


if simplification_percent != 0:
    option_simplify = f"-simplify {simplification_percent}% "
else:
    option_simplify = ""

cmd = (
    f"mapshaper {output_path}/raw.{format_intermediate} "
    f"{option_simplify}"
    f"-proj EPSG:{crs} "
    f"-each \"SOURCE='{provider}:{source}'\" "
    f"-split {dict_corresp[niveau_agreg]} "
    f"-o {output_path} format={format_output} extension=\".{format_output}\" singles"
   )



subprocess.run(
   cmd,
   shell=True
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


