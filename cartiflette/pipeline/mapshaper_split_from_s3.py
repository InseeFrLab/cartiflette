
import os

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import create_path_bucket
from cartiflette.mapshaper import mapshaperize_split, mapshaperize_split_merge
from .prepare_mapshaper import prepare_local_directory_mapshaper

def mapshaperize_split_from_s3(
    path_bucket,
    config,
    fs=FS
):

    format_output = config.get("format_output", "topojson")
    filter_by = config.get("filter_by", "DEPARTEMENT")
    borders = config.get("borders", "COMMUNE")
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

    local_directories = prepare_local_directory_mapshaper(
        path_bucket,
        borders=borders,
        niveau_agreg=filter_by,
        format_output=format_output,
        simplification=simplification,
        local_dir=local_dir,
        fs=fs
        )

    output_path = mapshaperize_split(
        local_dir=local_dir,
        filename_initial=borders,
        extension_initial="shp", 
        format_output=format_output,
        niveau_agreg=filter_by,
        provider=provider,
        source=source,
        year=year,
        dataset_family=dataset_family,
        territory=territory,
        crs=crs,
        simplification=simplification
    )

    for values in os.listdir(output_path):
        path_s3 = create_path_bucket(
                {
                    "bucket": bucket,
                    "path_within_bucket": path_within_bucket,
                    "year": year,
                    "borders": borders,
                    "crs": crs,
                    "filter_by": filter_by,
                    "value": values.replace(f".{format_output}", ""),
                    "vectorfile_format": format_output,
                    "provider": provider,
                    "dataset_family": dataset_family,
                    "source": source,
                    "territory": territory,
                    "simplification": simplification
                })
        fs.put(f"{output_path}/{values}", path_s3)


    return output_path


def mapshaperize_merge_split_from_s3(
    path_bucket,
    config,
    fs=FS
):

    format_output = config.get("format_output", "topojson")
    filter_by = config.get("filter_by", "DEPARTEMENT")
    borders = config.get("borders", "COMMUNE")
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

    local_directory = prepare_local_directory_mapshaper(
            path_bucket,
            borders="COMMUNE",
            niveau_agreg=filter_by,
            format_output=format_output,
            simplification=simplification,
            local_dir=local_dir,
            fs=fs
    )

    prepare_local_directory_mapshaper(
            path_bucket,
            borders="ARRONDISSEMENT_MUNICIPAL",
            niveau_agreg=filter_by,
            format_output=format_output,
            simplification=simplification,
            local_dir=local_dir,
            fs=fs
    )

    local_directory

    output_path = mapshaperize_split_merge(
        local_dir=local_dir,
        extension_initial="shp", 
        format_output=format_output,
        niveau_agreg=filter_by,
        provider=provider,
        source=source,
        year=year,
        dataset_family=dataset_family,
        territory=territory,
        crs=crs,
        simplification=simplification
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
                    "simplification": simplification
                })
        fs.put(f"{output_path}/{values}", path_s3)


    return output_path

