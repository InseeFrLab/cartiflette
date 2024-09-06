import tempfile


from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
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
            gis_file.create_downstream_geodatasets(
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

    provider = config.get("provider", "IGN")
    source = config.get("source", "EXPRESS-COG-CARTO-TERRITOIRE")
    year = config.get("year", 2022)
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
            gis_file.create_downstream_geodatasets_with_districts(
                metadata,
                format_output=format_output,
                niveau_agreg=filter_by,
                crs=crs,
                simplification=simplification,
            )


if __name__ == "__main__":
    mapshaperize_split_from_s3(
        {
            "year": 2023,
            "level_polygons": "COMMUNE",
            "filter_by": "FRANCE_ENTIERE",
            "format_output": "geojson",
            "simplification": 50,
        }
    )
