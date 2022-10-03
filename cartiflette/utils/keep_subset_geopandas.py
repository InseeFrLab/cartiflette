import geopandas as gpd

def keep_subset_geopandas(object, variable, values):
    if isinstance(values, (int, str, float)):
        return object.loc[object[variable] == values]
    else:
        return object.loc[object[variable].isin(values)]
