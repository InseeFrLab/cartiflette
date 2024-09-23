#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import logging
import re
import warnings

from diskcache import Cache
import pandas as pd
import s3fs

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET


cache = Cache("cartiflette-s3-cache", timeout=3600)
logger = logging.getLogger(__name__)


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
        raise KeyError
        return cache[("metadata", path_in_bucket)]
    except KeyError:
        pass

    try:
        with fs.open(path_in_bucket, mode="rb") as remote_file:
            remote = io.BytesIO(remote_file.read())
        if path_in_bucket.endswith("csv") or path_in_bucket.endswith("txt"):
            df = pd.read_csv(remote, **kwargs)

        elif path_in_bucket.endswith("xls") or path_in_bucket.endswith("xlsx"):
            try:
                df = pd.read_excel(remote, **kwargs)
            except ValueError:
                # try with calamine
                df = pd.read_excel(remote, engine="calamine", **kwargs)

    except Exception as e:
        warnings.warn(f"could not read {path_in_bucket=}: {e}")
        raise
    df.columns = [x.upper() for x in df.columns]

    # Remove 'ZZZZZ'-like values from INSEE datasets
    for col in df.columns:
        ix = df[df[col].str.fullmatch("Z+", case=False)].index
        df.loc[ix, col] = pd.NA

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

    def retrieve_path(provider, family: str, source: str, ext: str):
        path = (
            f"{bucket}/{path_within_bucket}/"
            f"provider={provider}/dataset_family={family}/source={source}"
            f"/year={year}/**/*.{ext}"
        )
        logger.debug(path)
        try:
            path = paths_bucket[(family, source)] = fs.glob(path)[0]
        except IndexError:
            warnings.warn(f"missing {family} {source} file for {year=}")

    for provider, family, source, ext in [
        ("Insee", "COG", "COMMUNE-OUTRE-MER", "csv"),
        ("Insee", "COG", "CANTON", "csv"),
        ("Insee", "COG", "COMMUNE", "csv"),
        ("Insee", "COG", "ARRONDISSEMENT", "csv"),
        ("Insee", "COG", "DEPARTEMENT", "csv"),
        ("Insee", "COG", "REGION", "csv"),
        ("Insee", "TAGC", "APPARTENANCE", "xlsx"),
        ("Insee", "TAGIRIS", "APPARTENANCE", "xlsx"),
        ("DGCL", "BANATIC", "CORRESPONDANCE-SIREN-INSEE-COMMUNES", "xlsx"),
        ("Insee", "INTERCOMMUNALITES", "EPCI-FP", "xlsx"),
        ("Insee", "INTERCOMMUNALITES", "EPT", "xlsx"),
    ]:
        retrieve_path(provider=provider, family=family, source=source, ext=ext)

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

    try:
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

    except Exception:
        cog_tom = None

    try:
        siren = s3_to_df(
            fs,
            paths_bucket[("BANATIC", "CORRESPONDANCE-SIREN-INSEE-COMMUNES")],
            **kwargs,
        )
        pop_communes = {
            "PTOT_[0-9]{4}": "POPULATION_TOTALE",
            "PMUN_[0-9]{4}": "POPULATION_MUNICIPALE",
            "PCAP_[0-9]{4}": "POPULATION_COMPTEE_A_PART",
        }
        rename = {
            col: new
            for pattern, new in pop_communes.items()
            for col in siren.columns
            if re.match(pattern, col)
        }
        rename.update({"SIREN": "SIREN_COMMUNE"})
        siren = siren.drop(["REG_COM", "DEP_COM", "NOM_COM"], axis=1).rename(
            rename, axis=1
        )
    except Exception:
        siren = None

    try:
        epci_fp = s3_to_df(
            fs,
            paths_bucket[("INTERCOMMUNALITES", "EPCI-FP")],
            skiprows=5,
            **kwargs,
        )
        epci_fp = epci_fp.dropna()
        epci_fp = epci_fp.loc[:, ["EPCI", "LIBEPCI"]].rename(
            {"LIBEPCI": "LIBELLE_EPCI"}, axis=1
        )
    except Exception:
        epci_fp = None

    try:
        ept = s3_to_df(
            fs,
            paths_bucket[("INTERCOMMUNALITES", "EPT")],
            skiprows=5,
            **kwargs,
        )
        ept = ept.dropna()
        ept = ept.loc[:, ["EPT", "LIBEPT"]].rename(
            {"LIBEPT": "LIBELLE_EPT"}, axis=1
        )
    except Exception:
        ept = None

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
        rename = {"DEPCOM": "CODE_ARM", "LIB_IRIS": "LIBELLE_IRIS"}
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

        # Add labels for EPCI-FP and EPT
        if epci_fp is not None:
            try:
                tagc = tagc.merge(epci_fp, on="EPCI", how="left")
            except KeyError:
                pass

        if ept is not None:
            try:
                tagc = tagc.merge(ept, on="EPT", how="left")
            except KeyError:
                pass

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

        if cog_tom is not None:
            cities = pd.concat([cities, cog_tom], ignore_index=True)

        cities = cities.merge(
            cog_arm.drop("TYPECOM", axis=1).rename(
                {
                    "COM": "CODE_ARM",
                    "LIBELLE": "LIBELLE_ARRONDISSEMENT_MUNICIPAL",
                },
                axis=1,
            ),
            how="left",
            left_on="CODGEO",
            right_on="COMPARENT",
        ).drop("COMPARENT", axis=1)
        ix = cities[cities.CODE_ARM.isnull()].index
        cities.loc[ix, "CODE_ARM"] = cities.loc[ix, "CODGEO"]
        cities.loc[ix, "LIBELLE_ARRONDISSEMENT_MUNICIPAL"] = cities.loc[
            ix, "LIBELLE_COMMUNE"
        ]
        # Set unique ARR code (as "NumDEP" + "NumARR") to ensure dissolution
        # is ok
        ix = cities[(cities.ARR.notnull())].index
        cities.loc[ix, "INSEE_ARR"] = (
            cities.loc[ix, "DEP"] + cities.loc[ix, "ARR"]
        )

        if siren is not None:
            cities = cities.merge(
                siren, how="left", left_on="CODGEO", right_on="INSEE"
            ).drop("INSEE", axis=1)

        cities["SOURCE_METADATA"] = "Cartiflette, d'après INSEE & DGCL"

    if iris is not None and cities is not None:
        iris_metadata = cities.merge(iris, on="CODE_ARM", how="left")
        iris_metadata = iris_metadata.drop(list(pop_communes.values()), axis=1)
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
        # TODO : Martinique (972) and Guyane (973) missing from CANTON

        cantons_metadata = cantons.merge(
            # Nota : we do not have the CANTON -> ARR imbrication as of yet
            # (except of course as a geospatial join...)
            cog_metadata.drop(
                ["ARR", "LIBELLE_ARRONDISSEMENT"], axis=1
            ).drop_duplicates(),
            on=["REG", "DEP"],
            how="inner",
        )
        keep = [
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
        cantons_metadata = cantons_metadata.loc[:, keep].rename(
            {"LIBELLE": "LIBELLE_CANTON"}, axis=1
        )

        # Hack to add PARIS :
        canton_paris = pd.DataFrame(
            [
                {
                    "INSEE_CAN": "NR",
                    "CAN": "NR",
                    "DEP": "75",
                    "REG": "11",
                    "LIBELLE_CANTON": "Paris",
                    "LIBELLE_DEPARTEMENT": "Paris",
                    "LIBELLE_REGION": "Île-de-France",
                }
            ]
        )
        cantons_metadata = pd.concat(
            [cantons_metadata, canton_paris], ignore_index=True
        )
        cantons_metadata["SOURCE_METADATA"] = "Cartiflette d'après INSEE"

    rename = {
        "DEP": "INSEE_DEP",
        "REG": "INSEE_REG",
        # "ARR": "INSEE_ARR", <- carefull, there is a INSEE_ARR already there!
        "CODGEO": "INSEE_COM",
        # "CAN": "INSEE_CAN", <- carefull, there is a INSEE_CAN already there!
        "CODE_ARM": "INSEE_ARM",
    }

    return_dict = {}
    ile_de_france = pd.DataFrame([{"REG": "11", "IDF": 1}])
    for label, df in [
        ("IRIS", iris_metadata),
        ("ARRONDISSEMENT_MUNICIPAL", cities_metadata),
        ("CANTON", cantons_metadata),
    ]:
        if df is not None:
            df = df.merge(ile_de_france, on="REG", how="left")
            df["IDF"] = df["IDF"].fillna(0).astype(int)
            df = df.rename(rename, axis=1)
        return_dict[label] = df
    return_dict

    return return_dict


# if __name__ == "__main__":
#     prepare_cog_metadata(2024)
