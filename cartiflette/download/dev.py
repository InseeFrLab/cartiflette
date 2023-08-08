"""
Request data from IGN and other tile providers
"""
from datetime import date
import geopandas as gpd
import numpy as np
import pandas as pd
import tempfile
import typing
import zipfile


from cartiflette import (
    import_yaml_config,
    # MasterScraper,
)


def get_vectorfile_ign(
    source: typing.Union[list, str] = ["EXPRESS-COG"],
    year: typing.Optional[str] = None,
    field: typing.Union[list, str] = [
        "metropole",
        "guadeloupe",
        "martinique",
        "reunion",
        "guyane",
        "mayotte",
    ],
    borders: typing.Union[list, str] = ["COMMUNE"],
    provider: typing.Union[list, str] = ["IGN", "opendatarchives"],
) -> gpd.GeoDataFrame:
    """
    User-level function to get shapefiles from IGN

    Args:
        source (typing.Union[list, str], optional): IGN data product. Defaults to ['EXPRESS-COG'].
        year (typing.Optional[str], optional): Year used. Defaults to None.
        field (typing.Union[list, str], optional): _description_.
           Defaults to "metropole". Acceptable values are "metropole",
           "guadeloupe", "martinique", "reunion", "guyane", "mayotte"].
        level (typing.Union[list, str], optional): Administrative level. Defaults to ['COMMUNE'].

    Returns:
        gpd.GeoDataFrame : _description_
    """

    if isinstance(source, list):
        source: str = source[0]

    if isinstance(borders, list):
        level: str = borders[0]

    if isinstance(provider, list):
        provider: str = provider[0]

    if isinstance(field, list):
        field = field[0]

    if year == 2019:
        field = "metropole"

    shp_location = store_vectorfile_ign(
        source=source, year=year, field=field, provider=provider
    )

    data_ign = gpd.read_file(f"{shp_location}/{borders}.shp")

    if borders == "ARRONDISSEMENT_MUNICIPAL":
        data_ign["INSEE_DEP"] = data_ign["INSEE_COM"].str[:2]

    data_ign["source"] = f"{provider}:{source}"

    return data_ign


def get_vectorfile_communes_arrondissement(
    year=None, provider="IGN", source="EXPRESS-COG-TERRITOIRE"
):
    if not year:
        year = date.today().year
    arrondissement = get_vectorfile_ign(
        borders="ARRONDISSEMENT_MUNICIPAL",
        year=year,
        field="metropole",
        provider=provider,
        source=source,
    )
    communes = get_vectorfile_ign(
        borders="COMMUNE",
        year=year,
        field="metropole",
        provider=provider,
        source=source,
    )
    communes_sans_grandes_villes = communes.loc[
        ~communes["NOM"].isin(["Marseille", "Lyon", "Paris"])
    ]
    communes_grandes_villes = communes.loc[
        communes["NOM"].isin(["Marseille", "Lyon", "Paris"])
    ]

    arrondissement_extra_info = arrondissement.merge(
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


def get_cog_year(year: int = None):
    # TODO : docstring et/ou deplacement vers download.py
    # Télécharge les fichiers COG à plat sur le site de l'INSEE et charge les
    # dataframes ad hoc ???
    if not year:
        year = date.today().year

    config = import_yaml_config()

    config_cog_year = config["Insee"]["COG"][year]
    config_root = config["Insee"]["COG"]["root"]
    url_root = f"{config_root}/{config_cog_year['id']}"

    urls = {
        cog["alias"]: f"{url_root}/{cog['filename']}"
        for key, cog in config_cog_year["content"].items()
    }

    # TODO : proxy, etc.
    dict_cog = {level: pd.read_csv(url) for level, url in urls.items()}

    return dict_cog


def get_BV(year: int = None):
    """
    Import and Unzip Bassins de vie (Insee, format 2012)

    Args:
        year

    Returns:
        A DataFrame
    """

    if not year:
        year = date.today().year

    dict_open_data = import_yaml_config()

    url = dict_open_data["Insee"]["BV2012"][year]["file"]

    out_name = safe_download_write(
        url,
        location=None,
        param_ftp=None,
        ext=".zip",
        verify=False,
        force=True,
    )

    tmp = tempfile.TemporaryDirectory()
    location = tmp.name
    # unzip in location directory

    archive = zipfile.ZipFile(out_name, "r")
    archive.extractall(path=location)
    archive.close()

    df = pd.read_excel(
        location + "/" + dict_open_data["Insee"]["BV2012"][year]["excel_name"],
        sheet_name="Composition_communale",
        skiprows=5,
    )
    df = df.loc[df["BV2012"] != "ZZZZZ"][["CODGEO", "BV2012"]]
    # ZZZZZ à Mayotte

    return df


if __name__ == "__main__":
    get_cog_year(year=2022)
