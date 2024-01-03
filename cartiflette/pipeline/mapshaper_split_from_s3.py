import os
import shutil

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket
from cartiflette.mapshaper import mapshaperize_split, mapshaperize_split_merge
from .prepare_mapshaper import prepare_local_directory_mapshaper


def mapshaperize_split_from_s3(config, fs=FS):
    format_output = config.get("format_output", "topojson")
    filter_by = config.get("filter_by", "DEPARTEMENT")
    territory = config.get("territory", "metropole")
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
            "dataset_family": "ADMINEXPRESS",
            "source": "EXPRESS-COG-CARTO-TERRITOIRE",
            "territory": "france",
            "filename": "raw.geojson",
            "simplification": 0,
        }
    )

    fs.download(path_raw_s3_combined, "temp/preprocessed_combined/COMMUNE.geojson")

    output_path = mapshaperize_split(
        local_dir=local_dir,
        config_file_city={
            "location": "temp/preprocessed_combined",
            "filename": "COMMUNE",
            "extension": "geojson",
        },
        format_output=format_output,
        niveau_agreg=filter_by,
        niveau_polygons=level_polygons,
        provider=provider,
        source=source,
        crs=crs,
        simplification=simplification,
    )

    for values in os.listdir(output_path):
        path_s3 = create_path_bucket(
            {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "borders": level_polygons,
                "crs": crs,
                "filter_by": filter_by,
                "value": values.replace(f".{format_output}", ""),
                "vectorfile_format": format_output,
                "provider": provider,
                "dataset_family": dataset_family,
                "source": source,
                "territory": territory,
                "simplification": simplification,
            }
        )
        fs.put(f"{output_path}/{values}", path_s3)

    shutil.rmtree(output_path)


def mapshaperize_merge_split_from_s3(config, fs=FS):
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
    local_dir = config.get("local_dir", "temp")

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
            "dataset_family": "ADMINEXPRESS",
            "source": "EXPRESS-COG-CARTO-TERRITOIRE",
            "territory": "france",
            "filename": "raw.geojson",
            "simplification": 0,
        }
    )

    fs.download(path_raw_s3_combined, "temp/preprocessed_combined/COMMUNE.geojson")

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
            "dataset_family": "ADMINEXPRESS",
            "source": "EXPRESS-COG-CARTO-TERRITOIRE",
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
        niveau_agreg=filter_by,
        format_output="topojson",
        simplification=simplification,
        local_dir="temp",
        fs=FS,
    )

    output_path = mapshaperize_split_merge(
        local_dir=local_dir,
        config_file_city={
            "location": "temp/preprocessed_combined",
            "filename": "COMMUNE",
            "extension": "geojson",
        },
        config_file_arrondissement={
            "location": "temp/metropole",
            "filename": "ARRONDISSEMENT_MUNICIPAL",
            "extension": "shp",
        },
        format_output=format_output,
        niveau_agreg=filter_by,
        provider=provider,
        source=source,
        crs=crs,
        simplification=simplification,
    )

    for values in os.listdir(output_path):
        path_s3 = create_path_bucket(
            {
                "bucket": bucket,
                "path_within_bucket": path_within_bucket,
                "year": year,
                "borders": "COMMUNE_ARRONDISSEMENT",
                "crs": crs,
                "filter_by": filter_by,
                "value": values.replace(f".{format_output}", ""),
                "vectorfile_format": format_output,
                "provider": provider,
                "dataset_family": dataset_family,
                "source": source,
                "territory": territory,
                "simplification": simplification,
            }
        )
        fs.put(f"{output_path}/{values}", path_s3)

    shutil.rmtree(output_path)
