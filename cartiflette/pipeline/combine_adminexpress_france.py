import os
import re
import shutil
import subprocess
from typing import Union

import s3fs

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET
from cartiflette.s3.dataset import BaseGISDataset, concat


COMPILED_TERRITORY = re.compile("territory=([a-z]*)/", flags=re.IGNORECASE)


def combine_adminexpress_territory(
    year: Union[str, int],
    intermediate_dir: str = "temp",
    format_intermediate: str = "topojson",
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
    format_intermediate : str, optional
        Temporary formats to use. The default is "topojson"
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
        "year": year,
    }

    path = (
        f"{bucket}/{path_within_bucket}/"
        "provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-TERRITOIRE/"
        f"year={year}/"
        "**/COMMUNE.*"
    )

    communes_paths = fs.glob(path)
    dirs = {os.path.dirname(x) for x in communes_paths}
    territories = {t for x in dirs for t in COMPILED_TERRITORY.findall(x)}

    if not territories:
        return

    print("Territoires récupérés:\n" + "\n".join(territories))

    datasets = [{"territory": territory} for territory in territories]
    for d in datasets:
        d.update(config)

    concat(
        [BaseGISDataset(fs=fs, **config) for config in datasets],
        format_intermediate=format_intermediate,
        fs=fs,
        **config,
    )

    return


# if __name__ == "__main__":
#     combine_adminexpress_territory(2022)
