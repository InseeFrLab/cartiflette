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
        warnings.warn(f"missing COG CANTON file for {year=}")
        path_bucket_cog_canton = None

    # Find ARRONDISSEMENT dataset on S3
    path = (
        f"{bucket}/{path_within_bucket}/"
        f"provider=Insee/dataset_family=COG/source=ARRONDISSEMENT/year={year}/"
        "**/*.csv"
    )
    try:
        path_bucket_cog_arrondissement = fs.glob(path)[0]
    except IndexError:
        warnings.warn(f"missing COG ARRONDISSEMENT file for {year=}")
        path_bucket_cog_arrondissements = None

    # Find DEPARTEMENT dataset on S3
    path = (
        f"{bucket}/{path_within_bucket}/"
        f"provider=Insee/dataset_family=COG/source=DEPARTEMENT/year={year}/"
        "**/*.csv"
    )
    try:
        path_bucket_cog_departement = fs.glob(path)[0]
    except IndexError:
        warnings.warn(f"missing COG DEPARTEMENT file for {year=}")
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
        warnings.warn(f"missing COG REGION file for {year=}")
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
        warnings.warn(f"missing TAGC APPARTENANCE file for {year=}")
        path_tagc = None

    # Find TAGIRIS APPARTENANCE dataset on S3
    path = (
        f"{bucket}/{path_within_bucket}/"
        f"provider=Insee/dataset_family=TAGIRIS/source=APPARTENANCE/"
        f"year={year}/**/*.xlsx"
    )
    try:
        path_tagiris = fs.glob(path)[0]
    except IndexError:
        warnings.warn(f"missing TAGIRIS APPARTENANCE file for {year=}")
        path_tagiris = None

    if any(
        x is None
        for x in (
            path_bucket_cog_region,
            path_bucket_cog_departement,
        )
    ):
        warnings.warn(f"{year=} metadata not constructed!")
        return

    def set_cols_to_uppercase(df):
        df.columns = [x.upper() for x in df.columns]

    dtype = "string[pyarrow]"
    with fs.open(path_bucket_cog_arrondissement, mode="rb") as remote_file:
        cog_ar = pd.read_csv(
            remote_file,
            dtype_backend="pyarrow",
            dtype={
                "ARR": dtype,
                "arr": dtype,
                "DEP": dtype,
                "dep": dtype,
                "REG": dtype,
                "reg": dtype,
            },
        )
        set_cols_to_uppercase(cog_ar)

    with fs.open(path_bucket_cog_departement, mode="rb") as remote_file:
        cog_dep = pd.read_csv(
            remote_file,
            dtype_backend="pyarrow",
            dtype={"DEP": dtype, "dep": dtype, "REG": dtype, "reg": dtype},
        )
        set_cols_to_uppercase(cog_dep)

    with fs.open(path_bucket_cog_region, mode="rb") as remote_file:
        cog_region = pd.read_csv(
            remote_file,
            dtype_backend="pyarrow",
            dtype={
                "REG": dtype,
                "reg": dtype,
            },
        )
        set_cols_to_uppercase(cog_region)

    # Merge ARR, DEPARTEMENT and REGION COG metadata
    cog_metadata = (
        cog_ar.loc[:, ["ARR", "DEP", "REG", "LIBELLE"]]
        .rename({"LIBELLE": "LIBELLE_ARRONDISSEMENT"})
        .merge(
            cog_dep.loc[:, ["DEP", "REG", "LIBELLE"]]
            .merge(
                cog_region.loc[:, ["REG", "LIBELLE"]],
                on="REG",
                suffixes=["_DEPARTEMENT", "_REGION"],
            )
            .drop(columns=["REG"]),
            on="DEP",
            how="outer",  # Nota : Mayotte not in ARR file
        )
    )

    # Compute metadata at IRIS level
    if path_tagiris is None:
        warnings.warn(f"{year=} metadata for iris not constructed!")
        iris = None

    else:
        # Read datasets from S3 into Pandas DataFrames
        with fs.open(path_tagiris, mode="rb") as remote_file:
            try:
                iris = pd.read_excel(
                    remote_file,
                    skiprows=5,
                    dtype_backend="pyarrow",
                    dtype={
                        "REG": dtype,
                        "DEP": dtype,
                        "CODE_IRIS": dtype,
                        "GRD_QUART": dtype,
                        "reg": dtype,
                        "dep": dtype,
                        "code_iris": dtype,
                        "grd_quart": dtype,
                    },
                )
                set_cols_to_uppercase(iris)
            except Exception as e:
                warnings.warn(f"could not read TAGIRIS file: {e}")
                warnings.warn(f"{year=} metadata for iris not constructed!")
                iris = None

            else:
                iris = iris.drop(columns=["LIBCOM", "UU2020", "REG", "DEP"])
                iris = iris.rename(
                    {"DEPCOM": "CODGEO", "LIB_IRIS": "LIBELLE_IRIS"}, axis=1
                )

    # Compute metadata at COMMUNE level
    if path_tagc is None:
        warnings.warn(f"{year=} metadata for cities/iris not constructed!")
        cities = None

    else:
        # Read datasets from S3 into Pandas DataFrames
        with fs.open(path_tagc, mode="rb") as remote_file:
            try:
                tagc = pd.read_excel(
                    remote_file,
                    skiprows=5,
                    dtype_backend="pyarrow",
                    dtype={
                        "REG": dtype,
                        "reg": dtype,
                    },
                )
            except Exception as e:
                warnings.warn(f"could not read TAGC file: {e}")
                warnings.warn(f"{year=} metadata for cities not constructed!")
                cities = None

            else:
                set_cols_to_uppercase(tagc)
                # Merge TAGC metadata with COG metadata
                cities = tagc.merge(cog_metadata)
                cities = cities.rename({"LIBGEO": "LIBELLE_COMMUNE"}, axis=1)
                cities["SOURCE_METADATA"] = "INSEE:COG"

    if iris is not None and cities is not None:
        iris_metadata = cities.merge(iris)
    else:
        iris_metadata = None
    if cities is not None:
        cities_metadata = cities
    else:
        cities_metadata = None

    # Compute metadata for CANTON
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
                    dtype={
                        "REG": dtype,
                        "reg": dtype,
                        "DEP": dtype,
                        "dep": dtype,
                    },
                )
                set_cols_to_uppercase(cantons)
            except Exception as e:
                warnings.warn(f"could not read CANTON file: {e}")
                warnings.warn(f"{year=} metadata for cantons not constructed!")
                cantons_metadata = None
            else:
                # Remove pseudo-cantons
                ix = cantons[cantons.COMPCT.isnull()].index
                cantons = cantons.drop(ix)

                # Set pure "CANTON" code (without dep part) to prepare for
                # join with IGN's CANTON geodataset
                cantons["INSEE_CAN"] = cantons["CAN"].str[-2:]

                # Merge CANTON metadata with COG metadata
                # TODO
                ## pb : Martinique (972) et Guyane (973) pas dans cantons

                cantons_metadata = cantons.merge(cog_metadata, how="inner")

                cantons_metadata = cantons_metadata.loc[
                    :,
                    [
                        "INSEE_CAN",
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

    return {
        "IRIS": iris_metadata,
        "COMMUNE": cities_metadata,
        "CANTON": cantons_metadata,
    }


if __name__ == "__main__":
    prepare_cog_metadata(2023)
