import os
import warnings

import pandas as pd
import s3fs

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET


def prepare_cog_metadata(
    year: int,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.core.S3FileSystem = FS,
) -> pd.DataFrame:
    """
    Prepares and retrieves COG (French Census Geographic Code) metadata by
    merging relevant datasets from raw sources stored on S3, such as
    DEPARTEMENT, REGION, and TAGC (Appartenance).

    Parameters:
    - year (int): The COG metadata's vintage
    - bucket (str): The bucket where the dataset are stored
    - path_within_bucket (str): The path within the S3 bucket where the datasets are stored.
    - fs (s3fs.core.S3FileSystem): An S3FileSystem object for interacting with the S3 bucket.

    Returns:
    - pd.DataFrame: A DataFrame containing the merged COG metadata, including DEPARTEMENT, REGION,
                    and TAGC information.
    """

    # TODO : calcul des tables BANATIC, etc.

    # Find CANTON dataset on S3
    path = (
        f"{bucket}/{path_within_bucket}/"
        f"provider=Insee/dataset_family=COG/source=CANTON/year={year}/"
        "**/*.csv"
    )
    try:
        path_bucket_cog_canton = fs.glob(path)[0]
    except IndexError:
        warnings.warn(f"missing CANTON file for {year=}")
        path_bucket_cog_canton = None

    # Find DEPARTEMENT dataset on S3
    path = (
        f"{bucket}/{path_within_bucket}/"
        f"provider=Insee/dataset_family=COG/source=DEPARTEMENT/year={year}/"
        "**/*.csv"
    )
    try:
        path_bucket_cog_departement = fs.glob(path)[0]
    except IndexError:
        warnings.warn(f"missing DEPARTEMENT file for {year=}")
        path_bucket_cog_departement = None

    # Find REGION dataset on S3
    path = (
        f"{bucket}/{path_within_bucket}/"
        f"provider=Insee/dataset_family=COG/source=REGION/year={year}/"
        "**/*.csv"
    )
    try:
        path_bucket_cog_region = fs.glob(path)[0]
    except IndexError:
        warnings.warn(f"missing REGION file for {year=}")
        path_bucket_cog_region = None

    # Find TAGC APPARTENANCE dataset on S3
    path = (
        f"{bucket}/{path_within_bucket}/"
        f"provider=Insee/dataset_family=TAGC/source=APPARTENANCE/year={year}/"
        "**/*.xlsx"
    )
    try:
        path_tagc = fs.glob(path)[0]
    except IndexError:
        warnings.warn(f"missing APPARTENANCE file for {year=}")
        path_tagc = None

    if any(
        x is None
        for x in (
            path_bucket_cog_region,
            path_bucket_cog_departement,
        )
    ):
        warnings.warn(f"{year=} metadata not constructed!")
        return

    with fs.open(path_bucket_cog_departement, mode="rb") as remote_file:
        cog_dep = pd.read_csv(
            remote_file,
            dtype_backend="pyarrow",
            dtype={"REG": "string[pyarrow]"},
        )

    with fs.open(path_bucket_cog_region, mode="rb") as remote_file:
        cog_region = pd.read_csv(
            remote_file,
            dtype_backend="pyarrow",
            dtype={"REG": "string[pyarrow]"},
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

    if path_tagc is None:
        warnings.warn(f"{year=} metadata for cities not constructed!")
        cities_metadata = None

    else:
        # Read datasets from S3 into Pandas DataFrames
        with fs.open(path_tagc, mode="rb") as remote_file:
            try:
                tagc = pd.read_excel(
                    remote_file,
                    skiprows=5,
                    dtype_backend="pyarrow",
                    dtype={"REG": "string[pyarrow]"},
                )
            except Exception as e:
                warnings.warn(f"could not read TAGC file: {e}")
                warnings.warn(f"{year=} metadata for cities not constructed!")
                cities_metadata = None

            else:
                # Merge TAGC metadata with COG metadata
                cities_metadata = tagc.merge(cog_metadata)
                cities_metadata = cities_metadata.drop(columns=["LIBGEO"])
                cities_metadata["SOURCE_METADATA"] = "INSEE:COG"

    if path_bucket_cog_canton is None:
        warnings.warn(f"{year=} metadata for cantons not constructed!")
        cantons_metadata = None

    else:
        # Read datasets from S3 into Pandas DataFrames
        with fs.open(path_bucket_cog_canton, mode="rb") as remote_file:
            try:
                cantons = pd.read_csv(
                    remote_file,
                    dtype_backend="pyarrow",
                    dtype={"REG": "string[pyarrow]", "DEP": "string[pyarrow]"},
                )
            except Exception as e:
                warnings.warn(f"could not read CANTON file: {e}")
                warnings.warn(f"{year=} metadata for cantons not constructed!")
                cantons_metadata = None
            else:
                # Merge CANTON metadata with COG metadata
                cantons_metadata = cantons.merge(cog_metadata, how="inner")
                cantons_metadata["SOURCE_METADATA"] = "INSEE:COG"

                cantons_metadata = cantons_metadata.loc[
                    :,
                    [
                        "CAN",
                        "DEP",
                        "REG",
                        "BURCENTRAL",
                        "TYPECT",
                        "LIBELLE",
                        "LIBELLE_DEPARTEMENT",
                        "LIBELLE_REGION",
                    ],
                ]

                cantons_metadata["SOURCE_METADATA"] = "INSEE:COG"

    return {"COMMUNE": cities_metadata, "CANTON": cantons_metadata}


if __name__ == "__main__":
    prepare_cog_metadata(2023)
