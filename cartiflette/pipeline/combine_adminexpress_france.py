import os
import re
from typing import Union
import warnings

import s3fs

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET
from cartiflette.s3.dataset import S3GeoDataset, concat


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
        Path of uploaded dataset on S3.

    """

    config = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "provider": "IGN",
        "dataset_family": "ADMINEXPRESS",
        "source": "EXPRESS-COG-CARTO-TERRITOIRE",
        "borders": None,
        "crs": "*",
        "filter_by": "origin",
        "value": "raw",
        "vectorfile_format": "shp",
        "simplification": 0,
        "year": year,
        "filename": "COMMUNE",
    }

    path = (
        f"{bucket}/{path_within_bucket}/"
        "provider=IGN/dataset_family=ADMINEXPRESS/"
        "source=EXPRESS-COG-CARTO-TERRITOIRE/"
        f"year={year}/"
        "administrative_level=None/"
        "crs=*/"
        "origin=raw/"
        "vectorfile_format=*/"
        "territory=*/"
        "simplification=*/"
        "COMMUNE.*"
    )

    communes_paths = fs.glob(path)
    dirs = {os.path.dirname(x) for x in communes_paths}
    territories = {t for x in dirs for t in COMPILED_TERRITORY.findall(x)}

    if not territories:
        warnings.warn(f"{year} not constructed (no territories)")
        return

    print("Territoires récupérés:\n" + "\n".join(territories))

    datasets = [{"territory": territory} for territory in territories]
    for d in datasets:
        d.update(config)

    config.update(
        {
            "vectorfile_format": format_intermediate,
            "crs": 4326,
            "borders": "france",
            "filter_by": "preprocessed",
            "value": "before_cog",
            "territory": "france",
        }
    )

    dset = concat(
        [S3GeoDataset(fs=fs, **config) for config in datasets],
        fs=fs,
        **config,
    )

    return dset.s3_dirpath


# if __name__ == "__main__":
#     combine_adminexpress_territory(2024)
