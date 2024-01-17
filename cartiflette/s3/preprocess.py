# -*- coding: utf-8 -*-

from datetime import date
from typing import TypedDict
import io
import logging
import pandas as pd
import s3fs

from cartiflette.config import BUCKET, PATH_WITHIN_BUCKET, FS
from cartiflette.utils import magic_csv_reader

logger = logging.getLogger(__name__)


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


# def get_BV(
#     year: int = None,
#     bv_source: str = "FondsDeCarte_BV_2022",
#     ign_source: str = "EXPRESS-COG-TERRITOIRE",
#     bucket: str = BUCKET,
#     path_within_bucket: str = PATH_WITHIN_BUCKET,
#     fs: s3fs.S3FileSystem = FS,
# ) -> gpd.GeoDataFrame:
#     """
#     Reconstruct living areas ("Bassins de vie") from AdminExpress' cities'
#     geometries and Insee's inventory.

#     Parameters
#     ----------
#     year : int, optional
#         Desired vintage. Will use the current year if set to None (which is
#         default).
#     bv_source : str, optional
#         Dataset's source to use for living area. The default is
#         "FondsDeCarte_BV_2022".
#     ign_source : str, optional
#         Dataset's source to use for geometries (should be a dataset from the
#         dataset_family AdminExpress. The default is "EXPRESS-COG-TERRITOIRE".
#     bucket : str, optional
#         Bucket to use. The default is BUCKET.
#     path_within_bucket : str, optional
#         path within bucket. The default is PATH_WITHIN_BUCKET.
#     fs : s3fs.S3FileSystem, optional
#         S3 file system to use. The default is FS.

#     Raises
#     ------
#     ValueError
#         If no file has been found on S3 for the given parameters.

#     Returns
#     -------
#     bv : gpd.GeoDataFrame
#         GeoDataFrame of living areas, constructed from cities geometries

#           Ex.:
#               bv              libbv dep reg  \
#         0  01004  Ambérieu-en-Bugey  01  84
#         1  01033         Valserhône  01  84
#         2  01033         Valserhône  74  84
#         3  01034             Belley  01  84
#         4  01053    Bourg-en-Bresse  01  84

#                                                     geometry  POPULATION
#         0  POLYGON ((5.31974 45.92194, 5.31959 45.92190, ...       46645
#         1  POLYGON ((5.72192 46.03413, 5.72165 46.03449, ...       25191
#         2  POLYGON ((5.85098 45.99099, 5.85094 45.99070, ...        4566
#         3  POLYGON ((5.61963 45.66754, 5.61957 45.66773, ...       25620
#         4  POLYGON ((5.18709 46.05114, 5.18692 46.05085, ...       83935

#     """

#     if not year:
#         year = date.today().year

#     territory = "france_entiere"
#     provider = "Insee"
#     dataset_family = "BV"
#     source = bv_source
#     pattern = (
#         f"{bucket}/{path_within_bucket}/{year=}/**/"
#         f"{provider=}/{dataset_family=}/{source=}/{territory=}/**/"
#         f"*.dbf"
#     ).replace("'", "")
#     files = fs.glob(pattern)  # , refresh=True)
#     # see issue : https://github.com/fsspec/s3fs/issues/504
#     if not files:
#         raise ValueError(
#             "No file retrieved with the set parameters, resulting to the "
#             f"following {pattern=}"
#         )
#     data = []
#     for file in files:
#         with tempfile.TemporaryDirectory() as tempdir:
#             tmp_dbf = os.path.join(tempdir, os.path.basename(file))
#             with open(tmp_dbf, "wb") as tf:
#                 with fs.open(file, "rb") as fsf:
#                     tf.write(fsf.read())

#             df = gpd.read_file(tmp_dbf, encoding="utf8")
#             df = df.drop("geometry", axis=1)
#         data.append(df)

#     bv = pd.concat(data)

#     communes = get_vectorfile_ign(
#         borders="COMMUNE",
#         year=year,
#         territory="*",
#         provider="IGN",
#         source=ign_source,
#     )

#     bv = communes.merge(bv, left_on="INSEE_COM", right_on="codgeo", how="right")
#     if bv_source == "FondsDeCarte_BV_2022":
#         rename = ["bv2022", "libbv2022"]
#     elif bv_source == "FondsDeCarte_BV_2012":
#         rename = ["bv2012", "libbv2012"]
#     bv = bv.rename(dict(zip(rename, ["bv", "libbv"])), axis=1)
#     by = ["bv", "libbv", "dep", "reg"]

#     bv = bv.dissolve(by=by, aggfunc={"POPULATION": "sum"}, as_index=False, dropna=False)

#     return bv


# if __name__ == "__main__":
# logging.basicConfig(level=logging.INFO)

# ret = get_cog_year(2022)
# ret = get_vectorfile_ign(source="EXPRESS-COG-TERRITOIRE", year=2022)
# ret = get_vectorfile_communes_arrondissement(year=2022)
# ret = get_BV(year=2022)
