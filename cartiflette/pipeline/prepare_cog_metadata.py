#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import logging
import re
import warnings

from diskcache import Cache
import pandas as pd
import numpy as np
import polars as pl
from pebble import ThreadPool
import s3fs

from cartiflette.config import FS, BUCKET, PATH_WITHIN_BUCKET, THREADS_DOWNLOAD


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
        return cache[("metadata", path_in_bucket)]
    except KeyError:
        pass

    try:

        with fs.open(path_in_bucket, mode="rb") as remote_file:
            remote = io.BytesIO(remote_file.read())
        if path_in_bucket.endswith("csv") or path_in_bucket.endswith("txt"):
            df = pl.read_csv(
                remote, infer_schema_length=0, **kwargs
            ).to_pandas()

        elif path_in_bucket.endswith("xls") or path_in_bucket.endswith("xlsx"):
            # carefull, with polars skip_rows and header_row are summed !
            kwargs = {"header_row": kwargs["skip_rows"]}
            df = pl.read_excel(
                remote,
                has_header=True,
                infer_schema_length=0,
                read_options=kwargs,
            ).to_pandas()

    except Exception as e:
        warnings.warn(f"could not read {path_in_bucket=}: {e}")
        raise
    df.columns = [x.upper() for x in df.columns]

    # Remove 'ZZZZZ'-like values from INSEE datasets
    for col in df.columns:
        ix = df[df[col].fillna("").str.fullmatch("Z+", case=False)].index
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

    # =========================================================================
    #     Part 1. : retrieve all paths on S3
    # =========================================================================
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

    args = [
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
        ("Insee", "POPULATION", "POPULATION-IRIS-COM", "xlsx"),
        ("Insee", "POPULATION", "POPULATION-IRIS-FRANCE-HORS-MAYOTTE", "xlsx"),
    ]
    if THREADS_DOWNLOAD > 1:
        with ThreadPool(THREADS_DOWNLOAD) as pool:
            list(pool.map(retrieve_path, *zip(*args)).result())
    else:
        for provider, family, source, ext in args:
            retrieve_path(
                provider=provider, family=family, source=source, ext=ext
            )

    try:
        [
            paths_bucket[("COG", x)]
            for x in ("REGION", "DEPARTEMENT", "ARRONDISSEMENT")
        ]
    except KeyError:
        warnings.warn(f"{year=} metadata not constructed!")
        return

    # =========================================================================
    #     Part 2. : download and read all datasets from S3
    # =========================================================================

    def download(key, skip_rows):
        try:
            path = paths_bucket[key]
            return key, s3_to_df(fs, path, skip_rows=skip_rows)
        except KeyError:
            # not there
            return key, pd.DataFrame()

    args = [
        (("COG", "COMMUNE"), 0),
        (("COG", "ARRONDISSEMENT"), 0),
        (("COG", "DEPARTEMENT"), 0),
        (("COG", "REGION"), 0),
        (("COG", "COMMUNE-OUTRE-MER"), 0),
        (("BANATIC", "CORRESPONDANCE-SIREN-INSEE-COMMUNES"), 0),
        (("POPULATION", "POPULATION-IRIS-FRANCE-HORS-MAYOTTE"), 5),
        (("POPULATION", "POPULATION-IRIS-COM"), 5),
        (("INTERCOMMUNALITES", "EPCI-FP"), 5),
        (("INTERCOMMUNALITES", "EPT"), 5),
        (("TAGIRIS", "APPARTENANCE"), 5),
        (("TAGC", "APPARTENANCE"), 5),
        (("COG", "CANTON"), 0),
    ]
    if THREADS_DOWNLOAD > 1:
        with ThreadPool(THREADS_DOWNLOAD) as pool:
            ddf = dict(pool.map(download, *zip(*args)).result())
    else:
        ddf = {key: download(key, skip)[-1] for key, skip in args}

    # Merge ARR, DEPARTEMENT and REGION COG metadata
    cog_metadata = (
        # Note : Mayotte (976) not in ARR
        # -> take DEP & REG from cog dep & cog reg
        ddf[("COG", "ARRONDISSEMENT")]
        .loc[:, ["ARR", "DEP", "LIBELLE"]]
        .rename({"LIBELLE": "LIBELLE_ARRONDISSEMENT"}, axis=1)
        .merge(
            ddf[("COG", "DEPARTEMENT")]
            .loc[:, ["DEP", "REG", "LIBELLE"]]
            .merge(
                ddf[("COG", "REGION")].loc[:, ["REG", "LIBELLE"]],
                on="REG",
                suffixes=["_DEPARTEMENT", "_REGION"],
            ),
            on="DEP",
            how="outer",  # Nota : Mayotte not in ARR file
        )
    )
    # Ex. cog_metadata :
    #    ARR DEP LIBELLE_ARRONDISSEMENT REG LIBELLE_DEPARTEMENT  \
    # 0  011  01                 Belley  84                 Ain
    # 1  012  01        Bourg-en-Bresse  84                 Ain

    #          LIBELLE_REGION
    # 0  Auvergne-Rhône-Alpes
    # 1  Auvergne-Rhône-Alpes

    # Compute metadata at COMMUNE level
    tagc = ddf[("TAGC", "APPARTENANCE")]
    if tagc.empty:
        warnings.warn(f"{year=} metadata for cities not constructed!")
        cities = pd.DataFrame()
        arm = pd.DataFrame()
    else:
        drop = {"CANOV", "CV"} & set(tagc.columns)
        tagc = tagc.drop(list(drop), axis=1)

        # Add labels for EPCI-FP
        epci_fp = ddf[("INTERCOMMUNALITES", "EPCI-FP")]
        if not epci_fp.empty:
            epci_fp = epci_fp.dropna()
            epci_fp = epci_fp.loc[:, ["EPCI", "LIBEPCI"]].rename(
                {"LIBEPCI": "LIBELLE_EPCI"}, axis=1
            )
            try:
                tagc = tagc.merge(epci_fp, on="EPCI", how="left")
            except KeyError:
                # EPCI column missing from TAGC
                pass

        # Add labels for EPT
        ept = ddf[("INTERCOMMUNALITES", "EPT")]
        if not ept.empty:
            ept = ept.dropna()
            ept = ept.loc[:, ["EPT", "LIBEPT"]].rename(
                {"LIBEPT": "LIBELLE_EPT"}, axis=1
            )
            try:
                tagc = tagc.merge(ept, on="EPT", how="left")
            except KeyError:
                # EPT column missing from TAGC
                pass

        cities = tagc.merge(
            cog_metadata, on=["ARR", "DEP", "REG"], how="inner"
        )
        cities = cities.rename({"LIBGEO": "LIBELLE_COMMUNE"}, axis=1)

        cog_tom = ddf[("COG", "COMMUNE-OUTRE-MER")]
        if not cog_tom.empty:
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
            cities = pd.concat([cities, cog_tom], ignore_index=True)

        cog_arm = ddf[("COG", "COMMUNE")].query("TYPECOM=='ARM'")
        cog_arm = cog_arm.loc[:, ["TYPECOM", "COM", "LIBELLE", "COMPARENT"]]

        arm = cities.merge(
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
        ix = arm[arm.CODE_ARM.isnull()].index
        arm.loc[ix, "CODE_ARM"] = arm.loc[ix, "CODGEO"]
        arm.loc[ix, "LIBELLE_ARRONDISSEMENT_MUNICIPAL"] = arm.loc[
            ix, "LIBELLE_COMMUNE"
        ]
        # Set unique ARR code (as "NumDEP" + "NumARR") to ensure dissolution
        # is ok
        for df in arm, cities:
            ix = df[(df.ARR.notnull())].index
            df.loc[ix, "INSEE_ARR"] = df.loc[ix, "DEP"] + df.loc[ix, "ARR"]

        siren = ddf[("BANATIC", "CORRESPONDANCE-SIREN-INSEE-COMMUNES")]
        if not siren.empty:
            pop_communes = {
                "PTOT_([0-9]{4})": "POPULATION_TOTALE",
                "PMUN_[0-9]{4}": "POPULATION_MUNICIPALE",
                "PCAP_[0-9]{4}": "POPULATION_COMPTEE_A_PART",
            }
            rename = {
                col: f"{new}_" + re.findall("[0-9]{4}", col)[0]
                for pattern, new in pop_communes.items()
                for col in siren.columns
                if re.match(pattern, col)
            }
            rename.update({"SIREN": "SIREN_COMMUNE"})
            siren = siren.drop(
                ["REG_COM", "DEP_COM", "NOM_COM"], axis=1
            ).rename(rename, axis=1)

            cities = cities.merge(
                siren, how="left", left_on="CODGEO", right_on="INSEE"
            ).drop("INSEE", axis=1)

            # Do not keep populations for ARM (info is not available on ARM
            # level for LYON or MARSEILLE)
            drop = {
                col: f"{new}_" + re.findall("[0-9]{4}", col)[0]
                for pattern, new in pop_communes.items()
                for col in siren.columns
                if re.match(pattern, col)
            }
            arm = arm.merge(
                siren[["SIREN_COMMUNE", "INSEE"]],
                how="left",
                left_on="CODGEO",
                right_on="INSEE",
            ).drop(
                ["INSEE"],
                axis=1,
            )
        for df in arm, cities:
            df["SOURCE_METADATA"] = "Cartiflette, d'après INSEE & DGCL"

    # Compute metadata at IRIS level
    iris = ddf[("TAGIRIS", "APPARTENANCE")]
    if iris.empty:
        warnings.warn(f"{year=} metadata for iris not constructed!")
        iris = pd.DataFrame()
    else:
        iris = iris.drop(columns=["LIBCOM", "UU2020", "REG", "DEP"])
        rename = {"DEPCOM": "CODE_ARM", "LIB_IRIS": "LIBELLE_IRIS"}
        iris = iris.rename(rename, axis=1)

        # retrieve populations
        pop_iris = pd.concat(
            [
                ddf[("POPULATION", "POPULATION-IRIS-FRANCE-HORS-MAYOTTE")],
                ddf[("POPULATION", "POPULATION-IRIS-COM")],
            ],
            ignore_index=True,
        )
        if pop_iris.empty:
            # all iris population dataframe are empty, triggering an exception
            pop_iris = pd.DataFrame()
        else:
            pop_iris_field = re.compile("P[0-9]{2}_POP$")
            pop_iris_field = [
                x for x in pop_iris.columns if pop_iris_field.match(x)
            ][0]
            pop_iris = pop_iris.loc[:, ["IRIS", pop_iris_field]].rename(
                {
                    pop_iris_field: "POPULATION_"
                    + re.findall("([0-9]{2})", pop_iris_field)[0]
                }
            )

        iris = arm.merge(iris, on="CODE_ARM", how="left")

    # Compute metadata at CANTON level
    cantons = ddf[("COG", "CANTON")]
    if cantons.empty:
        warnings.warn(f"{year=} metadata for cantons not constructed!")
    else:

        # Set pure "CANTON" code (without dep part) to prepare for
        # join with IGN's CANTON geodataset
        cantons["INSEE_CAN"] = cantons["CAN"].str[-2:]

        # Add Lyon if missing (<2024): single CANTON since creation of the
        # metropole, not covering the whole dept, so this should be added
        # before the merge operation like Paris, Martinique, etc.
        ix = cantons[
            (cantons.DEP == "69") & (cantons.NCC.str.contains("LYON"))
        ].index
        if ix.empty:
            cantons = pd.concat(
                [
                    cantons,
                    pd.DataFrame(
                        [
                            {
                                "CAN": "69NR",
                                "DEP": "69",
                                "REG": "84",
                                "INSEE_CAN": "NR",
                                "LIBELLE": "Lyon",
                                "INSEE_CAN": "NR",
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

        # Merge CANTON metadata with COG metadata
        cantons = cantons.merge(
            # Nota : we do not have the CANTON -> ARR imbrication as of yet
            # (except of course as a geospatial join...)
            cog_metadata.drop(
                ["ARR", "LIBELLE_ARRONDISSEMENT"], axis=1
            ).drop_duplicates(),
            on=["REG", "DEP"],
            # Note : Martinique (972) and Guyane (973) missing from CANTON
            # as well as Paris (75) for older vintages
            # -> go for outer join
            how="outer",
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
        cantons = cantons.loc[:, keep].rename(
            {"LIBELLE": "LIBELLE_CANTON"}, axis=1
        )

        # Hack to set PARIS, GUYANE and MARTINIQUE with the same key as IGN's
        # dataset (if trully missing)
        for dep, label in {
            "75": "Paris",  # missing for year <2024
            "973": "Guyane",
            "972": "Martinique",
        }.items():
            ix = cantons[cantons.DEP == dep].index
            if cantons.loc[ix, "CAN"].isnull().all():
                cantons.loc[ix, "INSEE_CAN"] = "NR"
                cantons.loc[ix, "CAN"] = (
                    cantons.loc[ix, "DEP"] + cantons.loc[ix, "INSEE_CAN"]
                )
                cantons.loc[ix, "LIBELLE_CANTON"] = label

        cantons["SOURCE_METADATA"] = "Cartiflette d'après INSEE"

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
        ("IRIS", iris),
        ("COMMUNE", cities),
        ("CANTON", cantons),
        ("ARRONDISSEMENT_MUNICIPAL", arm),
    ]:
        if not df.empty:
            df = df.replace(np.nan, pd.NA)
            df = df.merge(ile_de_france, on="REG", how="left")
            df["IDF"] = df["IDF"].fillna(0).astype(int)
            df = df.rename(rename, axis=1)
        return_dict[label] = df

    return return_dict
