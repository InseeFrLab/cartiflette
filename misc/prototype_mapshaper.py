from cartiflette.s3 import upload_s3_raw
from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.pipeline import mapshaperize_split_from_s3, mapshaperize_merge_split_from_s3
from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata
from cartiflette.download.download import _download_sources

path_within_bucket = "test-download23"


# DATA RETRIEVING STEP =========================


# IGN DATASET
path_bucket_adminexpress = upload_s3_raw(
    path_within_bucket=path_within_bucket,
    year=2022
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
        path_bucket_adminexpress,
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


