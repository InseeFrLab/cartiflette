import itertools
import pandas as pd


def restructure_nested_dict_borders(dict_with_list: dict):
    """
    Restructures a nested dictionary by flattening its values and their corresponding keys.

    Parameters:
    -----------
    dict_with_list : dict
        A dictionary with list values to be restructured.

    Returns:
    --------
    list
        A list of lists containing key-value pairs obtained by flattening the input dictionary.

    Example:
    --------
    Example usage:
        sample_dict = {'a': [1, 2, 3], 'b': [4, 5]}
        result = restructure_nested_dict_borders(sample_dict)
        print(result)

    This will output:
        [['a', 1], ['a', 2], ['a', 3], ['b', 4], ['b', 5]]
    """
    croisement_filter_by_borders_flat = [
        [key, inner_value]
        for key, values in dict_with_list.items()
        for inner_value in values
    ]

    return croisement_filter_by_borders_flat


def crossproduct_parameters_production(
    croisement_filter_by_borders: dict,
    list_format: list,
    years: list,
    crs_list: list,
    sources: list,
    simplifications: list,
) -> pd.DataFrame:
    """
    Generates a DataFrame by performing a cross-product of the given parameters.

    Parameters:
    -----------
    croisement_filter_by_borders : dict
        A dictionary with nested lists for cross-product generation.
    list_format : list
        A list of formats for cross-product generation.
    years : list
        A list of years for cross-product generation.
    crs_list : list
        A list of CRS (Coordinate Reference Systems) for cross-product generation.
    sources : list
        A list of sources for cross-product generation.
    simplifications : list
        A list of simplifications for cross-product generation.

    Returns:
    --------
    pd.DataFrame
        A pandas DataFrame containing the cross-product of the input parameters.

    Example:
    --------
    Example usage:
        sample_dict = {'a': [1, 2, 3], 'b': [4, 5]}
        formats = ['geojson', 'gpkg']
        years = [2022, 2022]
        crs_list = [4326, 2154]
        sources = ['source1', 'source2']
        simplifications = [0, 40]
        result = crossproduct_parameters_production(
            sample_dict, formats, years, crs_list, sources, simplifications
        )
        print(result)

    This will output:
        A pandas DataFrame with the cross-product of the provided parameters.
    """
    croisement_filter_by_borders_flat = restructure_nested_dict_borders(
        croisement_filter_by_borders
    )

    combinations = list(
        itertools.product(
            list_format,
            croisement_filter_by_borders_flat,
            years,
            crs_list,
            sources,
            simplifications,
        )
    )

    tempdf = pd.DataFrame(
        combinations,
        columns=["format_output", "nested", "year", "crs", "source", "simplification"],
    )
    tempdf["level_polygons"] = tempdf["nested"].apply(lambda tup: tup[0])
    tempdf["filter_by"] = tempdf["nested"].apply(lambda tup: tup[1])
    tempdf.drop("nested", axis="columns", inplace=True)

    return tempdf
