import cartiflette.s3 as s3

shp_communes = s3.download_vectorfile_url_all(
    values="metropole",
    crs=4326,
    borders="REGION",
    vectorfile_format="topojson",
    filter_by="FRANCE_ENTIERE",
    source="EXPRESS-COG-CARTO-TERRITOIRE",
    year=2022,
)

print(shp_communes.head())
