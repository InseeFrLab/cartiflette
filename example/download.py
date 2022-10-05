import cartiflette.s3

obj2 = cartiflette.s3.download_shapefile_s3_all(
    values = "11",
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2022)

print(obj2.head())

obj = cartiflette.s3.download_shapefile_s3_all(
    values = ["11","27","28"],
    level="COMMUNE",
    shapefile_format="geojson",
    decoupage="region",
    year=2022)

print(obj.head())