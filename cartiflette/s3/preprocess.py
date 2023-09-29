# -*- coding: utf-8 -*-

from datetime import date
import geopandas as gpd
import io
import logging
import numpy as np
import os
import pandas as pd
import s3fs
import tempfile
from typing import TypedDict


from cartiflette import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import magic_csv_reader

logger = logging.getLogger(__name__)

# TODO : docstrings


class CogDict(TypedDict):
    "Used only for typing hints"
    COMMUNE: pd.DataFrame
    CANTON: pd.DataFrame
    ARRONDISSEMENT: pd.DataFrame
    DEPARTEMENT: pd.DataFrame
    REGION: pd.DataFrame
    COLLECTIVITE: pd.DataFrame
    PAYS: pd.DataFrame


def get_cog_year(
    year: int = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> CogDict:
    """
    Retrieve all COG files on S3, concat all territories and store it into a
    dict

    Parameters
    ----------
    year : int, optional
        Desired vintage. If None (default), will use the current date's year.
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.

    Returns
    -------
    CogDict
        Dictionnary of dataframes. Each key represent a "layer" of the COG's
        yearly dataset. It might change from year to year, according to what's
        really present in the dataset.

        If no data is present for the desired vintage, empty dataframes will be
        returned in the dictionnary.

        Ex. :
        {
            'COMMUNE':
                       TYPECOM    COM   REG  ...                  LIBELLE    CAN COMPARENT
                 0         COM  01001  84.0  ...  L'Abergement-Clémenciat   0108       NaN
                 1         COM  01002  84.0  ...    L'Abergement-de-Varey   0101       NaN
                 2         COM  01004  84.0  ...        Ambérieu-en-Bugey   0101       NaN
                 3         COM  01005  84.0  ...      Ambérieux-en-Dombes   0122       NaN
                 4         COM  01006  84.0  ...                  Ambléon   0104       NaN
                 ...
                 [37601 rows x 12 columns],

            'CANTON':
                      id_canton id_departement  ...            libelle  actual
                 0         0101             01  ...  Ambérieu-en-Bugey       C
                 1         0102             01  ...           Attignat       C
                 2         0103             01  ...         Valserhône       C
                 3         0104             01  ...             Belley       C
                 4         0105             01  ...  Bourg-en-Bresse-1       C
                 ...
                 [2290 rows x 10 columns],

            'ARRONDISSEMENT':
                       ARR  DEP  ...                   NCCENR                  LIBELLE
                 0     011   01  ...                   Belley                   Belley
                 1     012   01  ...          Bourg-en-Bresse          Bourg-en-Bresse
                 2     013   01  ...                      Gex                      Gex
                 3     014   01  ...                   Nantua                   Nantua
                 4     021   02  ...          Château-Thierry          Château-Thierry
                 ...
                 [332 rows x 8 columns],

            'DEPARTEMENT':
                      DEP  REG  ...                   NCCENR                  LIBELLE
                 0     01   84  ...                      Ain                      Ain
                 1     02   32  ...                    Aisne                    Aisne
                 2     03   84  ...                   Allier                   Allier
                 3     04   93  ...  Alpes-de-Haute-Provence  Alpes-de-Haute-Provence
                 4     05   93  ...             Hautes-Alpes             Hautes-Alpes
                 ...
                 [101 rows x 7 columns],

            'REGION':
                     REG CHEFLIEU  ...                      NCCENR                  LIBELLE
                 0     1    97105  ...                  Guadeloupe               Guadeloupe
                 1     2    97209  ...                  Martinique               Martinique
                 2     3    97302  ...                      Guyane                   Guyane
                 3     4    97411  ...                  La Réunion               La Réunion
                 4     6    97608  ...                     Mayotte                  Mayotte
                 5    11    75056  ...               Île-de-France            Île-de-France
                 ...
                 [18 rows x 6 columns],

            'COLLECTIVITE':
                     CTCD  ...                                            LIBELLE
                 0    01D  ...                     Conseil départemental de L'Ain
                 1    02D  ...                   Conseil départemental de L'Aisne
                 2    03D  ...                  Conseil départemental de L'Allier
                 3    04D  ...  Conseil départemental des Alpes-de-Haute-Provence
                 4    05D  ...             Conseil départemental des Hautes-Alpes
                 ...
                 [100 rows x 6 columns],

            'PAYS':
                        COG  ACTUAL  CAPAY  CRPAY  ...  ANCNOM CODEISO2 CODEISO3 CODENUM3
                 0    99101       1    NaN    NaN  ...     NaN       DK      DNK    208.0
                 1    99101       3  99102    NaN  ...     NaN       FO      FRO    234.0
                 2    99102       1    NaN    NaN  ...     NaN       IS      ISL    352.0
                 3    99103       1    NaN    NaN  ...     NaN       NO      NOR    578.0
                 4    99103       3    NaN    NaN  ...     NaN       BV      BVT     74.0
                 ...
                 [282 rows x 11 columns]
         }

    """

    if not year:
        year = date.today().year

    levels = [
        "COMMUNE",
        "CANTON",
        "ARRONDISSEMENT",
        "DEPARTEMENT",
        "REGION",
        "COLLECTIVITE",
        "PAYS",
    ]

    dict_cog = {}
    for level in levels:
        pattern = (
            f"{bucket}/{path_within_bucket}/{year=}/**/"
            f"provider=Insee/dataset_family=COG/source={level}/**/*.*"
        )
        files = fs.glob(pattern)  # , refresh=True)
        # see issue : https://github.com/fsspec/s3fs/issues/504
        data = []
        for file in files:
            with fs.open(file, "rb") as f:
                dummy = io.BytesIO(f.read())
            df = magic_csv_reader(dummy)
            data.append(df)
        if data:
            dict_cog[level] = pd.concat(data)
        else:
            dict_cog[level] = pd.DataFrame()

    return dict_cog


def get_vectorfile_ign(
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-TERRITOIRE",
    year: str = None,
    territory: str = "metropole",
    borders: str = "COMMUNE",
    provider: str = "IGN",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> gpd.GeoDataFrame:
    """
    Retrieve IGN shapefiles from MinIO
    Note that each parameter from 'dataset_family' to 'provider' can be
    replaced by a joker using '*' instead.

    Parameters
    ----------
    dataset_family : str, optional
        Family as described in the yaml file. The default is "ADMINEXPRESS".
    source : str, optional
        Source as described in the yaml file. The default is
        "EXPRESS-COG-TERRITOIRE".
    year : int, optional
        Desired vintage. Will use the current year if set to None (which is
        default).
    territory : str, optional
        Territory as described in the yaml file. The default is "metropole".
    borders : str, optional
        Desired "mesh" (ie available layers in the raw dataset : commune,
        arrondissement, etc.). The default is "COMMUNE".
    provider : str, optional
        Provider described in the yaml file. The default is "IGN".
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.


    Raises
    ------
    ValueError
        If the dataset is not found on MinIO

    Returns
    -------
    gdf : gpd.GeoDataFrame
        Raw dataset as extracted from IGN. The projection/encoding of the file
        might have been standardised (ie projected to 4326 if the original
        projection was not EPSG referenced and encoded to UTF8)
        Ex. :
                                 ID           NOM         NOM_M INSEE_COM  \
        0  COMMUNE_0000000009754033    Connangles    CONNANGLES     43076
        1  COMMUNE_0000000009760784       Vidouze       VIDOUZE     65462
        2  COMMUNE_0000000009742077     Fouesnant     FOUESNANT     29058
        3  COMMUNE_0000000009735245  Plougrescant  PLOUGRESCANT     22218
        4  COMMUNE_0000000009752504     Montcarra     MONTCARRA     38250

                   STATUT  POPULATION INSEE_CAN INSEE_ARR INSEE_DEP INSEE_REG  \
        0  Commune simple         137        11         1        43        84
        1  Commune simple         243        13         3        65        76
        2  Commune simple        9864        11         4        29        53
        3  Commune simple        1166        27         3        22        53
        4  Commune simple         569        24         2        38        84

          SIREN_EPCI                                           geometry  \
        0  200073419  POLYGON ((748166.100 6463826.600, 748132.400 6...
        1  200072106  POLYGON ((455022.600 6263681.900, 455008.000 6...
        2  242900660  MULTIPOLYGON (((177277.800 6756845.800, 177275...
        3  200065928  MULTIPOLYGON (((245287.300 6878865.100, 245288...
        4  200068542  POLYGON ((889525.800 6504614.500, 889525.600 6...

                               source
        0  IGN:EXPRESS-COG-TERRITOIRE
        1  IGN:EXPRESS-COG-TERRITOIRE
        2  IGN:EXPRESS-COG-TERRITOIRE
        3  IGN:EXPRESS-COG-TERRITOIRE
        4  IGN:EXPRESS-COG-TERRITOIRE

    """

    if not year:
        year = date.today().year

    pattern = (
        f"{bucket}/{path_within_bucket}/{year=}/**/"
        f"{provider=}/{dataset_family=}/{source=}/{territory=}/**/"
        f"{borders}.shp"
    ).replace("'", "")
    files = fs.glob(pattern)  # , refresh=True)
    # see issue : https://github.com/fsspec/s3fs/issues/504
    if not files:
        raise ValueError(
            "No file retrieved with the set parameters, resulting to the "
            f"following {pattern=}"
        )

    data = []
    for file in files:
        logger.info(f"retrieving {file=}")
        with tempfile.TemporaryDirectory() as tempdir:
            pattern = file.rsplit(".", maxsplit=1)[0]
            all_files = fs.glob(pattern + "*")  # , refresh=True)
            # see issue : https://github.com/fsspec/s3fs/issues/504
            for temp in all_files:
                with open(
                    os.path.join(tempdir, os.path.basename(temp)), "wb"
                ) as tf:
                    with fs.open(temp, "rb") as fsf:
                        tf.write(fsf.read())
            gdf = gpd.read_file(os.path.join(tempdir, os.path.basename(file)))
        if len(files) > 1:
            # reproject all geodataframes before concatenation
            gdf = gdf.to_crs(4326)
        data.append(gdf)
    gdf = gpd.pd.concat(data)

    if borders == "ARRONDISSEMENT_MUNICIPAL":
        gdf["INSEE_DEP"] = gdf["INSEE_COM"].str[:2]

    gdf["source"] = f"{provider}:{source}"

    return gdf


def get_vectorfile_communes_arrondissement(
    year: int = None,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> gpd.GeoDataFrame:
    """
    Retrieved "enriched" dataframe for cities, using also cities' districts.

    Parameters
    ----------
    year : int, optional
        Desired vintage. Will use the current year if set to None (which is
        default).
    provider : str, optional
        Provider described in the yaml file. The default is "IGN".
    source : str, optional
        Source as described in the yaml file. The default is
        "EXPRESS-COG-TERRITOIRE".
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.
    Returns
    -------
    df_enrichi : gpd.GeoDataFrame
        Ex:
                                 ID           NOM         NOM_M INSEE_COM  \
        0  COMMUNE_0000000009754033    Connangles    CONNANGLES     43076
        1  COMMUNE_0000000009760784       Vidouze       VIDOUZE     65462
        2  COMMUNE_0000000009742077     Fouesnant     FOUESNANT     29058
        3  COMMUNE_0000000009735245  Plougrescant  PLOUGRESCANT     22218
        4  COMMUNE_0000000009752504     Montcarra     MONTCARRA     38250

                   STATUT  POPULATION INSEE_CAN INSEE_ARR INSEE_DEP INSEE_REG  \
        0  Commune simple         137        11         1        43        84
        1  Commune simple         243        13         3        65        76
        2  Commune simple        9864        11         4        29        53
        3  Commune simple        1166        27         3        22        53
        4  Commune simple         569        24         2        38        84

          SIREN_EPCI                                           geometry  \
        0  200073419  POLYGON ((748166.100 6463826.600, 748132.400 6...
        1  200072106  POLYGON ((455022.600 6263681.900, 455008.000 6...
        2  242900660  MULTIPOLYGON (((177277.800 6756845.800, 177275...
        3  200065928  MULTIPOLYGON (((245287.300 6878865.100, 245288...
        4  200068542  POLYGON ((889525.800 6504614.500, 889525.600 6...

                               source INSEE_COG
        0  IGN:EXPRESS-COG-TERRITOIRE     43076
        1  IGN:EXPRESS-COG-TERRITOIRE     65462
        2  IGN:EXPRESS-COG-TERRITOIRE     29058
        3  IGN:EXPRESS-COG-TERRITOIRE     22218
        4  IGN:EXPRESS-COG-TERRITOIRE     38250

    """

    if not year:
        year = date.today().year

    arrondissements = get_vectorfile_ign(
        borders="ARRONDISSEMENT_MUNICIPAL",
        year=year,
        territory="metropole",
        provider=provider,
        source=source,
    )

    communes = get_vectorfile_ign(
        borders="COMMUNE",
        year=year,
        territory="metropole",
        provider=provider,
        source=source,
    )
    communes_sans_grandes_villes = communes.loc[
        ~communes["NOM"].isin(["Marseille", "Lyon", "Paris"])
    ]
    communes_grandes_villes = communes.loc[
        communes["NOM"].isin(["Marseille", "Lyon", "Paris"])
    ]

    arrondissement_extra_info = arrondissements.merge(
        communes_grandes_villes, on="INSEE_DEP", suffixes=("", "_y")
    )
    arrondissement_extra_info = arrondissement_extra_info.loc[
        :, ~arrondissement_extra_info.columns.str.endswith("_y")
    ]

    df_enrichi = pd.concat(
        [communes_sans_grandes_villes, arrondissement_extra_info]
    )

    df_enrichi["INSEE_COG"] = np.where(
        df_enrichi["INSEE_ARM"].isnull(),
        df_enrichi["INSEE_COM"],
        df_enrichi["INSEE_ARM"],
    )

    df_enrichi = df_enrichi.drop("INSEE_ARM", axis="columns")

    return df_enrichi


def get_BV(
    year: int = None,
    bv_source: str = "FondsDeCarte_BV_2022",
    ign_source: str = "EXPRESS-COG-TERRITOIRE",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> gpd.GeoDataFrame:
    """
    Reconstruct living areas ("Bassins de vie") from AdminExpress' cities' 
    geometries and Insee's inventory.

    Parameters
    ----------
    year : int, optional
        Desired vintage. Will use the current year if set to None (which is
        default).
    bv_source : str, optional
        Dataset's source to use for living area. The default is 
        "FondsDeCarte_BV_2022".
    ign_source : str, optional
        Dataset's source to use for geometries (should be a dataset from the
        dataset_family AdminExpress. The default is "EXPRESS-COG-TERRITOIRE".
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.

    Raises
    ------
    ValueError
        If no file has been found on S3 for the given parameters.

    Returns
    -------
    bv : gpd.GeoDataFrame
        GeoDataFrame of living areas, constructed from cities geometries

          Ex.:
              bv              libbv dep reg  \
        0  01004  Ambérieu-en-Bugey  01  84
        1  01033         Valserhône  01  84
        2  01033         Valserhône  74  84
        3  01034             Belley  01  84
        4  01053    Bourg-en-Bresse  01  84

                                                    geometry  POPULATION
        0  POLYGON ((5.31974 45.92194, 5.31959 45.92190, ...       46645
        1  POLYGON ((5.72192 46.03413, 5.72165 46.03449, ...       25191
        2  POLYGON ((5.85098 45.99099, 5.85094 45.99070, ...        4566
        3  POLYGON ((5.61963 45.66754, 5.61957 45.66773, ...       25620
        4  POLYGON ((5.18709 46.05114, 5.18692 46.05085, ...       83935

    """

    if not year:
        year = date.today().year

    territory = "france_entiere"
    provider = "Insee"
    dataset_family = "BV"
    source = bv_source
    pattern = (
        f"{bucket}/{path_within_bucket}/{year=}/**/"
        f"{provider=}/{dataset_family=}/{source=}/{territory=}/**/"
        f"*.dbf"
    ).replace("'", "")
    files = fs.glob(pattern)  # , refresh=True)
    # see issue : https://github.com/fsspec/s3fs/issues/504
    if not files:
        raise ValueError(
            "No file retrieved with the set parameters, resulting to the "
            f"following {pattern=}"
        )
    data = []
    for file in files:
        with tempfile.TemporaryDirectory() as tempdir:
            tmp_dbf = os.path.join(tempdir, os.path.basename(file))
            with open(tmp_dbf, "wb") as tf:
                with fs.open(file, "rb") as fsf:
                    tf.write(fsf.read())

            df = gpd.read_file(tmp_dbf, encoding="utf8")
            df = df.drop("geometry", axis=1)
        data.append(df)

    bv = pd.concat(data)

    communes = get_vectorfile_ign(
        borders="COMMUNE",
        year=year,
        territory="*",
        provider="IGN",
        source=ign_source,
    )

    bv = communes.merge(
        bv, left_on="INSEE_COM", right_on="codgeo", how="right"
    )
    if bv_source == "FondsDeCarte_BV_2022":
        rename = ["bv2022", "libbv2022"]
    elif bv_source == "FondsDeCarte_BV_2012":
        rename = ["bv2012", "libbv2012"]
    bv = bv.rename(dict(zip(rename, ["bv", "libbv"])), axis=1)
    by = ["bv", "libbv", "dep", "reg"]

    bv = bv.dissolve(
        by=by, aggfunc={"POPULATION": "sum"}, as_index=False, dropna=False
    )

    return bv


if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)

    # ret = get_cog_year(2022)
    # ret = get_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2022)
    # ret = get_vectorfile_communes_arrondissement(year=2022)
    ret = get_BV(year=2022)
