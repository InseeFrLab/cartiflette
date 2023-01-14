import os

os.chdir("cartiflette")

import itertools
import cartiflette.s3 as s3
from cartiflette.download import get_administrative_level_available_ign

#formats = ["geoparquet", "shp", "gpkg", "geojson"]
formats = ["topojson"]
#level = ["ARRONDISSEMENT"]
years = [y for y in range(2021, 2023)]
#crs_list = [4326, 2154, "official"]
crs_list = [4326]

sources=["EXPRESS-COG-CARTO"]

croisement_decoupage_level = {
    ## structure -> niveau geo: [niveau decoupage macro],
    "COMMUNE": ["REGION", "DEPARTEMENT", "FRANCE_ENTIERE"]
    #"REGION": ["FRANCE_ENTIERE"]
    #"DEPARTEMENT":["REGION", "FRANCE_ENTIERE"],
}

tempdf = crossproduct_parameters_production(
    croisement_decoupage_level = croisement_decoupage_level,
    list_format = formats,
    years = years,
    crs_list = crs_list,
    sources = sources
)


for index, row in tempdf.iterrows():
    format = row['format']
    level = row['level']
    decoupage = row['decoupage']
    year = row['year']
    crs = row['crs']
    print(80*'==' + "\n" \
        f"level={level}\nvectorfile_format={format}\n" \
        f"decoupage={decoupage}\nyear={year}\n" \
        f"crs={crs}"
        )
    s3.write_vectorfile_s3_all(
        level=level,
        vectorfile_format=format,
        decoupage=decoupage,
        year=year,
        crs=crs,
        provider="IGN",
        source=source)


for format, couple_decoupage_level, year, epsg in itertools.product(
    formats, croisement_decoupage_level_flat, years, crs_list
    ):
    lev = couple_decoupage_level[0]
    decoup = couple_decoupage_level[1]
    print(80*'==' + "\n" \
        f"level={lev}\nvectorfile_format={format}\n" \
        f"decoupage={decoup}\nyear={year}\n" \
        f"crs={epsg}"
        )
    s3.write_vectorfile_s3_all(
        level=lev,
        vectorfile_format=format,
        decoupage=decoup,
        year=year,
        crs=epsg,
        provider="IGN",
        source=source)



#formats = ["geoparquet", "shp", "gpkg", "geojson"]
formats = ["geojson", "topojson"]
#decoupage = ["region", "departement"]
decoupage = ["departement"]
years = [y for y in range(2021, 2023)]
for format, decoup, year in itertools.product(
    formats, decoupage, years
    ):
    s3.write_vectorfile_s3_custom_arrondissement(
            vectorfile_format="geojson",
            decoupage="departement",
            crs=4326,
            year=year)


#formats = ["geoparquet", "shp", "gpkg", "geojson"]
formats = "geojson"
decoupage = ["france_entiere"]
level = ["COMMUNE", "ARRONDISSEMENT"]
years = [y for y in range(2021, 2023)]
for format, decoup, lev, year in itertools.product(
    formats, decoupage, level, years
    ):
    s3.write_vectorfile_s3_all(
        level=lev,
        vectorfile_format=format,
        decoupage=decoup,
        year=year)




# OLD --------------


s3.write_vectorfile_s3_all(
        level="ARRONDISSEMENT_MUNICIPAL",
        vectorfile_format="geojson",
        decoupage="departement",
        year=2022)


obj = s3.download_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022,
    values = ["28","11"])

get_administrative_level_available_ign()


s3.write_vectorfile_s3_all(
    level="ARRONDISSEMENT",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

obj2 = s3.download_vectorfile_s3_single(
    level="COMMUNE",
    vectorfile_format="gpkg",
    decoupage="region",
    year=2022)

obj3 = s3.download_vectorfile_s3_single(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2022)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

s3.write_vectorfile_s3_all(
    level="ARRONDISSEMENT",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2021)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="GPKG",
    decoupage="region",
    year=2019)

s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="parquet",
    decoupage="region",
    year=2019)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2020)


s3.write_vectorfile_s3_all(
    level="COMMUNE",
    vectorfile_format="shp",
    decoupage="region",
    year=2022)