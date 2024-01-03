from cartiflette.s3 import upload_s3_raw
from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.pipeline import mapshaperize_split_from_s3, mapshaperize_merge_split_from_s3
from cartiflette.download.download import _download_sources


import subprocess

from cartiflette.utils import import_yaml_config
from cartiflette.pipeline.prepare_mapshaper import prepare_local_directory_mapshaper
from cartiflette import FS, BUCKET, PATH_WITHIN_BUCKET


# DATA RETRIEVING STEP =========================

path_within_bucket = "test-download27"

from cartiflette import BUCKET, PATH_WITHIN_BUCKET, FS, DICT_CORRESP_IGN
from cartiflette.utils import create_path_bucket
from cartiflette.mapshaper.mapshaper_split import mapshaper_enrich
from cartiflette.pipeline.combine_adminexpress_france import combine_adminexpress_territory
from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata


bucket=BUCKET
path_within_bucket="test-download27"
year=2022
fs=FS
#path_within_bucket = PATH_WITHIN_BUCKET

# PART 1/ COMBINE RAW FILES TOGETHER AND WRITE TO S3

path_combined_files = combine_adminexpress_territory(
    path_within_bucket=path_within_bucket
)

path_raw_s3 = create_path_bucket(
            {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "borders": "france",
                "crs": 4326,
                "filter_by": "preprocessed",
                "value": "before_cog",
                "vectorfile_format": "geojson",
                "provider": "IGN",
                "dataset_family": 'ADMINEXPRESS',
                "source": 'EXPRESS-COG-CARTO-TERRITOIRE',
                "territory": "france",
                "filename": "raw.geojson",
                "simplification": 0,
            }
        )

fs.put_file(path_combined_files, path_raw_s3)


# STEP 2: ENRICH AND SPLIT ----------------------

# Retrieve COG metadata
tagc_metadata = prepare_cog_metadata(
    path_within_bucket
)
tagc_metadata.drop(columns=["LIBGEO"]).to_csv("temp/tagc.csv")


from cartiflette.mapshaper.mapshaper_split import mapshaperize_split, mapshaperize_split_merge

config = {
        'path_within_bucket': path_within_bucket,
        "level_polygons": "COMMUNE",
        "filter_by": "REGION",
        "simplification": 50
    }

    format_output = config.get("format_output", "topojson")
    filter_by = config.get("filter_by", "DEPARTEMENT")
    territory = config.get("territory", "metropole")

    provider = config.get("provider", "IGN")
    source = config.get("source", "EXPRESS-COG-CARTO-TERRITOIRE")
    year = config.get("year", 2022)
    dataset_family = config.get("dataset_family", "ADMINEXPRESS")
    territory = config.get("territory", "metropole")
    crs = config.get("crs", 4326)
    simplification = config.get("simplification", 0)

local_dir = "temp"
niveau_agreg = filter_by
level_polygons = "COMMUNE"

path_raw_s3_combined = create_path_bucket(
            {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "borders": "france",
                "crs": 4326,
                "filter_by": "preprocessed",
                "value": "before_cog",
                "vectorfile_format": "geojson",
                "provider": "IGN",
                "dataset_family": 'ADMINEXPRESS',
                "source": 'EXPRESS-COG-CARTO-TERRITOIRE',
                "territory": "france",
                "filename": "raw.geojson",
                "simplification": 0,
            }
        )

fs.download(
    path_raw_s3_combined,
    "temp/preprocessed_combined/COMMUNE.geojson"
)

path_raw_s3_arrondissement = create_path_bucket(
            {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "borders": None,
                "crs": 2154,
                "filter_by": "origin",
                "value": "raw",
                "vectorfile_format": "shp",
                "provider": "IGN",
                "dataset_family": 'ADMINEXPRESS',
                "source": 'EXPRESS-COG-CARTO-TERRITOIRE',
                "territory": "metropole",
                "filename": "ARRONDISSEMENT_MUNICIPAL.shp",
                "simplification": 0,
            }
        )
path_raw_s3_arrondissement = path_raw_s3_arrondissement.rsplit("/", maxsplit=1)[0]

# retrieve arrondissement
prepare_local_directory_mapshaper(
    path_raw_s3_arrondissement,
    borders="ARRONDISSEMENT_MUNICIPAL",
    territory="metropole",
    niveau_agreg=niveau_agreg,
    format_output="topojson",
    simplification=simplification,
    local_dir="temp",
    fs=FS,
)

# pipeline communes

output_path = mapshaperize_split(
        local_dir=local_dir,
        config_file_city={
            "location": "temp/preprocessed_combined",
            "filename": "COMMUNE",
            "extension": "geojson"
        },
        format_output=format_output,
        niveau_agreg=filter_by,
        niveau_polygons=level_polygons,
        provider=provider,
        source=source,
        crs=crs,
        simplification=simplification,
    )


output_path = mapshaperize_split_merge(
        local_dir=local_dir,
        config_file_city={
            "location": "temp/preprocessed_combined",
            "filename": "COMMUNE",
            "extension": "geojson"
        },
        config_file_arrondissement = {
            "location": "temp/metropole",
            "filename": "ARRONDISSEMENT_MUNICIPAL",
            "extension": "shp"
        },
        format_output=format_output,
        niveau_agreg=filter_by,
        provider=provider,
        source=source,
        crs=crs,
        simplification=simplification,
    )


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
