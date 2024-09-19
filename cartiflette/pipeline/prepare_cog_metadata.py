import warnings

import pandas as pd
import s3fs

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET

from diskcache import Cache

cache = Cache("cartiflette-s3-cache", timeout=3600)


def s3_to_df(
    fs: s3fs.S3FileSystem, path_in_bucket: str, **kwargs
) -> pd.DataFrame:
    """
    Retrieve DataFrame from S3 with cache handling.

    Parameters
    ----------
    fs : s3fs.S3FileSystem
        An S3FileSystem object for interacting with the S3 bucket
    path_in_bucket : str
        Target file's path on S3 bucket
    **kwargs :
        Optionnal kwargs to pass to either pandas.read_excel or pandas.read_csv

    Returns
    -------
    df : pd.DataFrame
        Download dataset as dataframe

    """

    try:
        return cache[("metadata", path_in_bucket)]
    except KeyError:
        pass

    try:
        with fs.open(path_in_bucket, mode="rb") as remote_file:
            if path_in_bucket.endswith("csv") or path_in_bucket.endswith(
                "txt"
            ):
                method = pd.read_csv
            elif path_in_bucket.endswith("xls") or path_in_bucket.endswith(
                "xlsx"
            ):
                method = pd.read_excel
            df = method(remote_file, **kwargs)
    except Exception as e:
        warnings.warn(f"could not read {path_in_bucket=}: {e}")
        raise

    df.columns = [x.upper() for x in df.columns]
    cache[("metadata", path_in_bucket)] = df

    return df


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

    paths_bucket = {}

    def retrieve_path(family: str, source: str, ext: str):
        path = (
            f"{bucket}/{path_within_bucket}/"
            f"provider=Insee/dataset_family={family}/source={source}"
            f"/year={year}/**/*.{ext}"
        )
        try:
            path = paths_bucket[(family, source)] = fs.glob(path)[0]
        except IndexError:
            warnings.warn(f"missing {family} {source} file for {year=}")

    for family, source, ext in [
        ("COG", "COMMUNE-OUTRE-MER", "csv"),
        ("COG", "CANTON", "csv"),
        ("COG", "COMMUNE", "csv"),
        ("COG", "ARRONDISSEMENT", "csv"),
        ("COG", "DEPARTEMENT", "csv"),
        ("COG", "REGION", "csv"),
        ("TAGC", "APPARTENANCE", "xlsx"),
        ("TAGIRIS", "APPARTENANCE", "xlsx"),
    ]:
        retrieve_path(family=family, source=source, ext=ext)

    try:
        [
            paths_bucket[("COG", x)]
            for x in ("REGION", "DEPARTEMENT", "ARRONDISSEMENT")
        ]
    except KeyError:
        warnings.warn(f"{year=} metadata not constructed!")
        return

    def set_cols_to_uppercase(df):
        df.columns = [x.upper() for x in df.columns]

    kwargs = {"dtype_backend": "pyarrow", "dtype": "string[pyarrow]"}
    cog_com = s3_to_df(fs, paths_bucket[("COG", "COMMUNE")], **kwargs)

    cog_arm = cog_com.query("TYPECOM=='ARM'")
    cog_arm = cog_arm.loc[:, ["TYPECOM", "COM", "LIBELLE", "COMPARENT"]]

    cog_ar = s3_to_df(fs, paths_bucket[("COG", "ARRONDISSEMENT")], **kwargs)
    cog_dep = s3_to_df(fs, paths_bucket[("COG", "DEPARTEMENT")], **kwargs)
    cog_reg = s3_to_df(fs, paths_bucket[("COG", "REGION")], **kwargs)
    cog_tom = s3_to_df(
        fs, paths_bucket[("COG", "COMMUNE-OUTRE-MER")], **kwargs
    )

    keep = ["COM_COMER", "LIBELLE", "COMER", "LIBELLE_COMER"]
    cog_tom = cog_tom.query("NATURE_ZONAGE=='COM'").loc[:, keep]

    cog_tom = cog_tom.rename(
        {
            "COMER": "DEP",
            "LIBELLE_COMER": "LIBELLE_DEPARTEMENT",
            "COM_COMER": "CODGEO",
            "LIBELLE": "LIBELLE_COMMUNE",
        },
        axis=1,
    )

    # Merge ARR, DEPARTEMENT and REGION COG metadata
    cog_metadata = (
        # Note : Mayotte (976) not in ARR -> take DEP & REG from cog_dep & cog_reg
        cog_ar.loc[:, ["ARR", "DEP", "LIBELLE"]]
        .rename({"LIBELLE": "LIBELLE_ARRONDISSEMENT"}, axis=1)
        .merge(
            cog_dep.loc[:, ["DEP", "REG", "LIBELLE"]].merge(
                cog_reg.loc[:, ["REG", "LIBELLE"]],
                on="REG",
                suffixes=["_DEPARTEMENT", "_REGION"],
            ),
            on="DEP",
            how="outer",  # Nota : Mayotte not in ARR file
        )
    )

    # Compute metadata at IRIS level
    try:
        path = paths_bucket[("TAGIRIS", "APPARTENANCE")]
        iris = s3_to_df(fs, path, skiprows=5, **kwargs)
    except Exception:
        warnings.warn(f"{year=} metadata for iris not constructed!")
        iris = None
    else:
        iris = iris.drop(columns=["LIBCOM", "UU2020", "REG", "DEP"])
        rename = {"DEPCOM": "CODGEO", "LIB_IRIS": "LIBELLE_IRIS"}
        iris = iris.rename(rename, axis=1)

    # Compute metadata at COMMUNE level
    try:
        path = paths_bucket[("TAGC", "APPARTENANCE")]
        tagc = s3_to_df(fs, path, skiprows=5, **kwargs)
    except Exception:
        warnings.warn(f"{year=} metadata for cities not constructed!")
        cities = None
    else:
        drop = {"CANOV", "CV"} & set(tagc.columns)
        tagc = tagc.drop(list(drop), axis=1)

        for col in tagc.columns:
            ix = tagc[tagc[col].str.fullmatch("Z+", case=False)].index
            tagc.loc[ix, col] = pd.NA
        cities = tagc.merge(
            cog_metadata, on=["ARR", "DEP", "REG"], how="inner"
        )

        # Hack while Mayotte is missing from COG ARRONDISSEMENT
        mayotte = (
            tagc.merge(
                cities[["CODGEO", "LIBELLE_DEPARTEMENT"]],
                on="CODGEO",
                how="outer",
            )
            .query("LIBELLE_DEPARTEMENT.isnull()")
            .drop("LIBELLE_DEPARTEMENT", axis=1)
            .merge(
                cog_metadata.drop("ARR", axis=1),
                on=["DEP", "REG"],
                how="inner",
            )
        )

        cities = pd.concat([cities, mayotte], ignore_index=True)
        cities = cities.rename({"LIBGEO": "LIBELLE_COMMUNE"}, axis=1)
        cities = pd.concat([cities, cog_tom], ignore_index=True)

        cities["SOURCE_METADATA"] = "INSEE:COG"

        # TODO : add ARM

    if iris is not None and cities is not None:
        iris_metadata = cities.merge(iris)
    else:
        iris_metadata = None
    if cities is not None:
        cities_metadata = cities
    else:
        cities_metadata = None

    # Compute metadata for CANTON
    try:
        cantons = s3_to_df(fs, paths_bucket[("COG", "CANTON")], **kwargs)
    except Exception:
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
        keep = [
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
            ]
        ]
        cantons_metadata = cantons_metadata.loc[:, keep]
        cantons_metadata["SOURCE_METADATA"] = "INSEE:COG"

    return {
        "IRIS": iris_metadata,
        "COMMUNE": cities_metadata,
        "CANTON": cantons_metadata,
    }


if __name__ == "__main__":
    prepare_cog_metadata(2023)
