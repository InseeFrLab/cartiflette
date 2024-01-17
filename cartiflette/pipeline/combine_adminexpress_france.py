import subprocess

from cartiflette.config import FS, PATH_WITHIN_BUCKET
from cartiflette.utils import import_yaml_config
from cartiflette.mapshaper import mapshaper_convert_mercator
from cartiflette.s3 import upload_s3_raw
from .prepare_mapshaper import prepare_local_directory_mapshaper


def combine_adminexpress_territory(
    intermediate_dir="temp", path_within_bucket=PATH_WITHIN_BUCKET, fs=FS
):
    local_dir = intermediate_dir
    format_intermediate = "geojson"

    yaml = import_yaml_config()

    list_territories = yaml["IGN"]["ADMINEXPRESS"]["EXPRESS-COG-TERRITOIRE"][
        "territory"
    ].keys()

    list_location_raw = {
        territ: upload_s3_raw(
            path_within_bucket=path_within_bucket, year=2022, territory=territ
        )
        for territ in list_territories
    }

    for territory, path_bucket in list_location_raw.items():
        prepare_local_directory_mapshaper(
            path_bucket,
            borders="COMMUNE",
            territory=territory,
            niveau_agreg="COMMUNE",
            format_output="geojson",
            simplification=0,
            local_dir=local_dir,
            fs=fs,
        )

    for territ in list_territories:
        mapshaper_convert_mercator(
            local_dir=local_dir, territory=territ, identifier=territ
        )

    output_path = f"{local_dir}/preprocessed_combined/raw.{format_intermediate}"

    subprocess.run(
        (
            f"mapshaper -i {local_dir}/preprocessed/*.geojson combine-files name='COMMUNE' "
            f"-proj EPSG:4326 "
            f"-merge-layers "
            f"-o {output_path} "
            f'format={format_intermediate} extension=".{format_intermediate}" singles'
        ),
        shell=True,
        check=True,
    )

    return output_path
