import os
import re
import subprocess


from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET
from cartiflette.s3 import BaseGISDataset


COMPILED_YEAR = re.compile("year=([0-9]{4})")
COMPILED_TERRITORY = re.compile("territory=([a-z]*)/", flags=re.IGNORECASE)


def combine_adminexpress_territory(
    intermediate_dir="temp",
    bucket=BUCKET,
    path_within_bucket=PATH_WITHIN_BUCKET,
    fs=FS,
):
    config = {
        "bucket": BUCKET,
        "path_within_bucket": PATH_WITHIN_BUCKET,
        "provider": "IGN",
        "dataset_family": "ADMINEXPRESS",
        "source": "EXPRESS-COG-TERRITOIRE",
        "borders": None,
        "crs": "*",
        "filter_by": "origin",
        "value": "raw",
        "vectorfile_format": "shp",
        "simplification": 0,
        "intermediate_dir": intermediate_dir,
    }
    path = (
        f"{bucket}/{path_within_bucket}/"
        "provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-TERRITOIRE/**/COMMUNE.*"
    )

    format_intermediate = "geojson"

    communes_paths = fs.glob(path)
    dirs = {os.path.dirname(x) for x in communes_paths}
    years = {y for x in dirs for y in COMPILED_YEAR.findall(x)}
    territories = {t for x in dirs for t in COMPILED_TERRITORY.findall(x)}

    for year in years:
        for territory in territories:
            with BaseGISDataset(
                year=year, territory=territory, **config
            ) as dset:
                dset.to_mercator()

        output_path = f"{intermediate_dir}/preprocessed_combined/raw.{format_intermediate}"

        subprocess.run(
            (
                f"mapshaper -i {intermediate_dir}/preprocessed/*.geojson combine-files name='COMMUNE' "
                f"-proj EPSG:4326 "
                f"-merge-layers "
                f"-o {output_path} "
                f'format={format_intermediate} extension=".{format_intermediate}" singles'
            ),
            shell=True,
            check=True,
        )
        raise Exception("Stopping here !")

    return output_path


if __name__ == "__main__":
    combine_adminexpress_territory()
