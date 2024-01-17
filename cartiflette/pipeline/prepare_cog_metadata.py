import os
import pandas as pd
import s3fs

from cartiflette.config import FS
from cartiflette.s3 import upload_s3_raw


def prepare_cog_metadata(
    path_within_bucket: str, local_dir: str = "temp", fs: s3fs.core.S3FileSystem = FS
) -> pd.DataFrame:
    """
    Prepares and retrieves COG (French Census Geographic Code) metadata by fetching and merging
    relevant datasets from remote sources, such as DEPARTEMENT, REGION, and TAGC (Appartenance).

    Parameters:
    - path_within_bucket (str): The path within the S3 bucket where the datasets will be stored.
    - local_dir (str): Local directory where the datasets will be downloaded.
    - fs (s3fs.core.S3FileSystem): An S3FileSystem object for interacting with the S3 bucket.

    Returns:
    - pd.DataFrame: A DataFrame containing the merged COG metadata, including DEPARTEMENT, REGION,
                    and TAGC information.
    """

    # Create the local directory if it does not exist
    os.makedirs(local_dir, exist_ok=True)

    # Fetch and upload DEPARTEMENT dataset to S3
    path_bucket_cog_departement = upload_s3_raw(
        provider="Insee",
        dataset_family="COG",
        source="DEPARTEMENT",
        territory="france_entiere",
        borders="DATASET_INSEE_COG_DEPARTEMENT_FRANCE_ENTIERE_2022",
        year=2022,
        vectorfile_format="csv",
        path_within_bucket=path_within_bucket,
    )

    # Fetch and upload REGION dataset to S3
    path_bucket_cog_region = upload_s3_raw(
        provider="Insee",
        dataset_family="COG",
        source="REGION",
        territory="france_entiere",
        borders="DATASET_INSEE_COG_REGION_FRANCE_ENTIERE_2022",
        year=2022,
        vectorfile_format="csv",
        path_within_bucket=path_within_bucket,
    )

    # Fetch and upload TAGC APPARTENANCE dataset to S3
    path_bucket_tagc_appartenance = upload_s3_raw(
        provider="Insee",
        dataset_family="TAGC",
        source="APPARTENANCE",
        territory="france_entiere",
        borders="table-appartenance-geo-communes-22",
        year=2022,
        vectorfile_format="xlsx",
        path_within_bucket=path_within_bucket,
    )

    # Retrieve paths for the uploaded datasets
    path_tagc = fs.ls(path_bucket_tagc_appartenance)[0]
    path_bucket_cog_departement = fs.ls(path_bucket_cog_departement)[0]
    path_bucket_cog_region = fs.ls(path_bucket_cog_region)[0]

    # Read datasets from S3 into Pandas DataFrames
    with fs.open(path_tagc, mode="rb") as remote_file:
        tagc = pd.read_excel(
            remote_file,
            skiprows=5,
            dtype_backend="pyarrow",
            dtype={"REG": "string[pyarrow]"},
        )

    with fs.open(path_bucket_cog_departement, mode="rb") as remote_file:
        cog_dep = pd.read_csv(
            remote_file, dtype_backend="pyarrow", dtype={"REG": "string[pyarrow]"}
        )

    with fs.open(path_bucket_cog_region, mode="rb") as remote_file:
        cog_region = pd.read_csv(
            remote_file, dtype_backend="pyarrow", dtype={"REG": "string[pyarrow]"}
        )

    # Merge DEPARTEMENT and REGION COG metadata
    cog_metadata = (
        cog_dep.loc[:, ["DEP", "REG", "LIBELLE"]]
        .merge(
            cog_region.loc[:, ["REG", "LIBELLE"]],
            on="REG",
            suffixes=["_DEPARTEMENT", "_REGION"],
        )
        .drop(columns=["REG"])
    )

    # Merge TAGC metadata with COG metadata
    tagc_metadata = tagc.merge(cog_metadata)

    return tagc_metadata
