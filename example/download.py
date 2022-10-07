import cartiflette.s3

obj2 = cartiflette.s3.download_vectorfile_s3_all(
    values = "11",
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

print(obj2.head())

obj = cartiflette.s3.download_vectorfile_s3_all(
    values = ["11","27","28"],
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

print(obj.head())

normandie = cartiflette.s3.download_vectorfile_url_all(
    values = "11",
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

print(normandie.head())

regions = cartiflette.s3.download_vectorfile_url_all(
    values = ["11","27","28"],
    level="COMMUNE",
    vectorfile_format="geojson",
    decoupage="region",
    year=2022)

print(regions.head())
