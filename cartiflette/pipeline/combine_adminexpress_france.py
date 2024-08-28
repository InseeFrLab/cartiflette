import os
import re
import shutil
import subprocess
from typing import Union

import s3fs

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET
from cartiflette.s3 import BaseGISDataset


COMPILED_TERRITORY = re.compile("territory=([a-z]*)/", flags=re.IGNORECASE)


def combine_adminexpress_territory(
    year: Union[str, int],
    intermediate_dir: str = "temp",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> str:
    """
    Merge cities datasets into a single file (full France territory).

    All files are retrieved from S3, projected to mercator coordinates, then
    merged using mapshaper. Every computation is done on the disk, inside
    a temporary file.

    Parameters
    ----------
    year : Union[str, int]
        Desired vintage
    intermediate_dir : str, optional
        Temporary dir to process files. The default is "temp".
    bucket : str, optional
        Storage bucket on S3 FileSystem. The default is BUCKET.
    path_within_bucket : str, optional
        Path within S3 bucket used for storage. The default is
        PATH_WITHIN_BUCKET.
    fs : s3fs.FyleSystem, optional
        S3 file system used for storage of raw data. The default is FS.

    Returns
    -------
    output_path : str
        Path of merged dataset. Should be
        f"{intermediate_dir}/{year}/preprocessed_combined/raw.geojson"

    """

    config = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "provider": "IGN",
        "dataset_family": "ADMINEXPRESS",
        "source": "EXPRESS-COG-TERRITOIRE",
        "borders": None,
        "crs": "*",
        "filter_by": "origin",
        "value": "raw",
        "vectorfile_format": "shp",
        "simplification": 0,
    }

    path = (
        f"{bucket}/{path_within_bucket}/"
        "provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-TERRITOIRE/"
        f"year={year}/"
        "**/COMMUNE.*"
    )

    format_intermediate = "geojson"

    communes_paths = fs.glob(path)
    dirs = {os.path.dirname(x) for x in communes_paths}
    territories = {t for x in dirs for t in COMPILED_TERRITORY.findall(x)}

    try:
        for territory in territories:
            with BaseGISDataset(
                fs=fs,
                intermediate_dir=f"{intermediate_dir}/{year}",
                year=year,
                territory=territory,
                **config,
            ) as dset:
                dset.to_mercator()

        output_path = (
            f"{intermediate_dir}/{year}/preprocessed_combined/"
            f"raw.{format_intermediate}"
        )
        
        

        subprocess.run(
            (
                f"mapshaper -i {intermediate_dir}/{year}/preprocessed/*.geojson"
                " combine-files name='COMMUNE' "
                f"-proj EPSG:4326 "
                f"-merge-layers "
                f"-o {output_path} "
                f"format={format_intermediate} "
                f'extension=".{format_intermediate}" singles'
            ),
            shell=True,
            check=True,
        )
    except Exception:
        raise
    finally:
        shutil.rmtree(
            f"{intermediate_dir}/{year}/preprocessed", ignore_errors=True
        )

    return output_path


# if __name__ == "__main__":
#     combine_adminexpress_territory(2022)
