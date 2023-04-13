"""Wrapper to subset geopandas dataframes

"""
import typing
import geopandas as gpd


def keep_subset_geopandas(
    obj: gpd.GeoDataFrame, variable: str, values: typing.Union[list, str, int, float]
) -> gpd.GeoDataFrame:
    """A utility to subset gpd.GeoDataFrame

    Args:
        obj (gpd.GeoDataFrame): GeoPandas dataset that should be subsetted
        variable (str): Variable to use for subsetting
        values (typing.Union[list, str, int, float]): Value(s) that should be used.
            Can be a numeric value, a string or a list

    Returns:
        gpd.GeoDataFrame: _description_
    """

    if isinstance(values, (int, str, float)):
        return obj.loc[obj[variable] == values]

    return obj.loc[obj[variable].isin(values)]
