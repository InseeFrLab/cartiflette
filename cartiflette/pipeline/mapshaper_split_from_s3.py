import os
import shutil
import tempfile


from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket
from cartiflette.mapshaper import mapshaperize_split_merge
from cartiflette.s3 import S3GeoDataset, S3Dataset


def mapshaperize_split_from_s3(
    config, format_intermediate: str = "topojson", fs=FS
):
    format_output = config.get("format_output", "topojson")
    filter_by = config.get("filter_by", "DEPARTEMENT")
    level_polygons = config.get("level_polygons", "COMMUNE")

    provider = config.get("provider", "IGN")
    source = config.get("source", "EXPRESS-COG-CARTO-TERRITOIRE")
    year = config.get("year", 2024)
    dataset_family = config.get("dataset_family", "ADMINEXPRESS")
    crs = config.get("crs", 4326)
    simplification = config.get("simplification", 0)

    bucket = config.get("bucket", BUCKET)
    path_within_bucket = config.get("path_within_bucket", PATH_WITHIN_BUCKET)

    with tempfile.TemporaryDirectory() as tempdir:
        kwargs = {
            "fs": fs,
            "local_dir": tempdir,
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "year": year,
            "borders": "france",
            "filter_by": "preprocessed",
            "territory": "france",
        }
        with S3Dataset(
            provider="Insee",
            dataset_family="COG-TAGC",
            source="COG-TAGC",
            crs=None,
            value="tagc",
            vectorfile_format="csv",
            **kwargs,
        ) as metadata, S3GeoDataset(
            provider=provider,
            dataset_family=dataset_family,
            source=source,
            crs=4326,
            value="before_cog",
            vectorfile_format=format_intermediate,
            **kwargs,
        ) as gis_file:
            gis_file.mapshaperize_split(
                metadata,
                format_output=format_output,
                niveau_agreg=filter_by,
                niveau_polygons=level_polygons,
                crs=crs,
                simplification=simplification,
            )


def mapshaperize_merge_split_from_s3(
    config, format_intermediate: str = "topojson", fs=FS
):
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

    bucket = config.get("bucket", BUCKET)
    path_within_bucket = config.get("path_within_bucket", PATH_WITHIN_BUCKET)

    with tempfile.TemporaryDirectory() as tempdir:
        kwargs = {
            "fs": fs,
            "local_dir": tempdir,
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "year": year,
            "borders": "france",
            "filter_by": "preprocessed",
            "territory": "france",
        }
        with S3Dataset(
            provider="Insee",
            dataset_family="COG-TAGC",
            source="COG-TAGC",
            crs=None,
            value="tagc",
            vectorfile_format="csv",
            **kwargs,
        ) as metadata, S3GeoDataset(
            provider=provider,
            dataset_family=dataset_family,
            source=source,
            crs=4326,
            value="before_cog",
            vectorfile_format=format_intermediate,
            **kwargs,
        ) as gis_file:
            gis_file.mapshaperize_merge_split(
                metadata,
                format_output=format_output,
                niveau_agreg=filter_by,
                crs=crs,
                simplification=simplification,
            )

    # path_raw_s3_combined = create_path_bucket(
    #     {
    #         "bucket": bucket,
    #         "path_within_bucket": path_within_bucket,
    #         "year": year,
    #         "borders": "france",
    #         "crs": 4326,
    #         "filter_by": "preprocessed",
    #         "value": "before_cog",
    #         "vectorfile_format": "geojson",
    #         "provider": "IGN",
    #         "dataset_family": "ADMINEXPRESS",
    #         "source": "EXPRESS-COG-CARTO-TERRITOIRE",
    #         "territory": "france",
    #         "filename": "raw.geojson",
    #         "simplification": 0,
    #     }
    # )

    # fs.download(
    #     path_raw_s3_combined, "temp/preprocessed_combined/COMMUNE.geojson"
    # )

    # with S3GeoDataset(
    #     fs=fs,
    #     intermediate_dir="temp",
    #     bucket=bucket,
    #     path_within_bucket=path_within_bucket,
    #     provider="IGN",
    #     dataset_family="ADMINEXPRESS",
    #     source="EXPRESS-COG-CARTO-TERRITOIRE",
    #     year=year,
    #     borders=None,
    #     crs=2154,
    #     filter_by="origin",
    #     value="raw",
    #     vectorfile_format="shp",
    #     territory="metropole",
    #     simplification=0,
    # ) as dset:
    #     dset.to_local_folder_for_mapshaper()

    # output_path = mapshaperize_split_merge(
    #     local_dir=local_dir,
    #     config_file_city={
    #         "location": "temp/preprocessed_combined",
    #         "filename": "COMMUNE",
    #         "extension": "geojson",
    #     },
    #     config_file_arrondissement={
    #         "location": "temp/metropole",
    #         "filename": "ARRONDISSEMENT_MUNICIPAL",
    #         "extension": "shp",
    #     },
    #     format_output=format_output,
    #     niveau_agreg=filter_by,
    #     provider=provider,
    #     source=source,
    #     crs=crs,
    #     simplification=simplification,
    # )

    # for values in os.listdir(output_path):
    #     path_s3 = create_path_bucket(
    #         {
    #             "bucket": bucket,
    #             "path_within_bucket": path_within_bucket,
    #             "year": year,
    #             "borders": "COMMUNE_ARRONDISSEMENT",
    #             "crs": crs,
    #             "filter_by": filter_by,
    #             "value": values.replace(f".{format_output}", ""),
    #             "vectorfile_format": format_output,
    #             "provider": provider,
    #             "dataset_family": dataset_family,
    #             "source": source,
    #             "territory": territory,
    #             "simplification": simplification,
    #         }
    #     )
    #     fs.put(f"{output_path}/{values}", path_s3)

    # shutil.rmtree(output_path)


if __name__ == "__main__":
    mapshaperize_merge_split_from_s3(
        {
            "year": 2023,
            "level_polygons": "COMMUNE",
            "filter_by": "REGION",
        }
    )
