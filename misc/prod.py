from cartiflette import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket
from cartiflette.pipeline.combine_adminexpress_france import (
    combine_adminexpress_territory,
)
from cartiflette.pipeline.prepare_cog_metadata import prepare_cog_metadata

from cartiflette.pipeline import crossproduct_parameters_production
from cartiflette.pipeline import (
    mapshaperize_split_from_s3,
    mapshaperize_merge_split_from_s3,
)

# DATA RETRIEVING STEP =========================

bucket = BUCKET
path_within_bucket = PATH_WITHIN_BUCKET
year = 2022
fs = FS


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
        "dataset_family": "ADMINEXPRESS",
        "source": "EXPRESS-COG-CARTO-TERRITOIRE",
        "territory": "france",
        "filename": "raw.geojson",
        "simplification": 0,
    }
)

fs.put_file(path_combined_files, path_raw_s3)

# Retrieve COG metadata
tagc_metadata = prepare_cog_metadata(path_within_bucket)
tagc_metadata.drop(columns=["LIBGEO"]).to_csv("temp/tagc.csv")


# STEP 2: ENRICH AND SPLIT ----------------------
# First a few tests


croisement_decoupage_level = {
    # structure -> niveau geo: [niveau decoupage macro],
    "COMMUNE": [
        "BASSIN_VIE", "ZONE_EMPLOI", "UNITE_URBAINE", "AIRE_ATTRACTION_VILLES",  # zonages d'études
        "DEPARTEMENT", "REGION",  # zonages administratifs
        "TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "DEPARTEMENT": ["REGION", "TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "REGION": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "BASSIN_VIE": ["TERRITOIRE", "FRANCE_ENTIERE"],
    "ZONE_EMPLOI": ["TERRITOIRE", "FRANCE_ENTIERE"],
    "UNITE_URBAINE": ["TERRITOIRE", "FRANCE_ENTIERE"],
    "AIRE_ATTRACTION_VILLES": ["TERRITOIRE", "FRANCE_ENTIERE"]
}


formats = ["topojson", "geojson"]

# years = [y for y in range(2021, 2023)]
years = [2022]

# crs_list = [4326, 2154, "official"]
# crs_list = [4326, 2154]
crs_list = [4326]

sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]


tempdf = crossproduct_parameters_production(
    croisement_filter_by_borders=croisement_decoupage_level,
    list_format=formats,
    years=years,
    crs_list=crs_list,
    sources=sources,
    simplifications=[0, 50],
)

for index, row in tempdf.iterrows():
    print(row)
    mapshaperize_split_from_s3(
        {**{"path_within_bucket": path_within_bucket}, **row.to_dict()},
    )


# niveau commune_arrondissement

tempdf_arr = tempdf.loc[tempdf["level_polygons"] == "COMMUNE"].copy()
tempdf_arr = tempdf_arr.drop(columns=["level_polygons"])

for index, row in tempdf_arr.iterrows():
    print(row)
    mapshaperize_merge_split_from_s3(
        {**{"path_within_bucket": path_within_bucket}, **row.to_dict()},
    )
