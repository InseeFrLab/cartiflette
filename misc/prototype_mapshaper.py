from cartiflette.s3 import upload_s3_raw
from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.pipeline import mapshaperize_split_from_s3, mapshaperize_merge_split_from_s3
from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata
from cartiflette.download.download import _download_sources

path_within_bucket = "test-download27"


from cartiflette.utils import import_yaml_config
from cartiflette.pipeline.prepare_mapshaper import prepare_local_directory_mapshaper
from cartiflette import FS

fs = FS

config = {
    'path_within_bucket': path_within_bucket,
    "level_polygons": "COMMUNE",
    "filter_by": "REGION",
    "simplification": 50
}

yaml = import_yaml_config()

list_territories = yaml['IGN']['ADMINEXPRESS']['EXPRESS-COG-TERRITOIRE']['territory'].keys()

list_location_raw = {
    territ: upload_s3_raw(path_within_bucket=path_within_bucket, year=2022, territory=territ) for territ in list_territories
}

    format_output = config.get("format_output", "topojson")
    filter_by = config.get("filter_by", "DEPARTEMENT")
    borders = config.get("borders", "COMMUNE")
    level_polygons = config.get("level_polygons", "COMMUNE")
    territory = config.get("territory", "metropole")

    provider = config.get("provider", "IGN")
    source = config.get("source", "EXPRESS-COG-CARTO-TERRITOIRE")
    year = config.get("year", 2022)
    dataset_family = config.get("dataset_family", "ADMINEXPRESS")
    territory = config.get("territory", "metropole")
    crs = config.get("crs", 4326)
    simplification = config.get("simplification", 0)

    bucket = config.get("bucket", BUCKET)
    path_within_bucket = config.get("path_within_bucket", PATH_WITHIN_BUCKET)
    local_dir = config.get("local_dir", "temp")

for territory, path_bucket in list_location_raw.items():
    prepare_local_directory_mapshaper(
            path_bucket,
            borders=borders,
            territory=territory,
            niveau_agreg=filter_by,
            format_output=format_output,
            simplification=simplification,
            local_dir=local_dir,
            fs=fs,
        )


# DATA RETRIEVING STEP =========================


# IGN DATASETS

path_bucket_adminexpress = upload_s3_raw(
    path_within_bucket=path_within_bucket,
    year=2022
)


essai = upload_s3_raw(
    path_within_bucket=path_within_bucket,
    year=2022,
    territory="blabla"
)


# Retrieve COG metadata
tagc_metadata = prepare_cog_metadata(
    path_within_bucket
)
tagc_metadata.drop(columns=["LIBGEO"]).to_csv("temp/tagc.csv")



# TEST MAPSHAPERIZE

mapshaperize_split_from_s3(
    path_bucket_adminexpress,
    {
        'path_within_bucket': path_within_bucket,
        "level_polygons": "COMMUNE",
        "filter_by": "REGION",
        "simplification": 50
    }
)

mapshaperize_split_from_s3(
    path_bucket_adminexpress,
    {
        'path_within_bucket': path_within_bucket,
        "level_polygons": "REGION",
        "filter_by": "FRANCE_ENTIERE",
        "simplification": 50,
        "crs": 2154
    }
)


mapshaperize_split_from_s3(
    path_bucket_adminexpress,
    {
        'path_within_bucket': path_within_bucket,
        "level_polygons": "DEPARTEMENT",
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
    "REGION": ["FRANCE_ENTIERE"],
    #"ARRONDISSEMENT_MUNICIPAL" : ['DEPARTEMENT'], 
    #"COMMUNE_ARRONDISSEMENT": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "COMMUNE": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "DEPARTEMENT": ["REGION", "FRANCE_ENTIERE"]
}


formats = ["topojson", "geojson"]
formats = ["geojson"]

years = [y for y in range(2021, 2023)]
#years = [2022]

#crs_list = [4326, 2154, "official"]
#crs_list = [4326, 2154]
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
        path_bucket_adminexpress,
        {
            **{'path_within_bucket': path_within_bucket},
            **row.to_dict()
        }
    )


# niveau commune_arrondissement

tempdf_arr = tempdf.loc[tempdf['level_polygons'] == "COMMUNE"].copy()
tempdf_arr = tempdf_arr.drop(columns = ['level_polygons'])

for index, row in tempdf_arr.iterrows():
    print(row)
    mapshaperize_merge_split_from_s3(
        path_bucket_adminexpress,
        {
            **{'path_within_bucket': path_within_bucket},
            **row.to_dict()
        }
    )
