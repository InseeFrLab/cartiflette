from .dict_correspondance import (
    dict_corresp_filter_by,
    create_format_standardized,
    create_format_driver,
)


def standardize_inputs(vectorfile_format):
    corresp_filter_by_columns = dict_corresp_filter_by()
    format_standardized = create_format_standardized()
    gpd_driver = create_format_driver()
    format_write = format_standardized[vectorfile_format.lower()]
    driver = gpd_driver[format_write]

    return corresp_filter_by_columns, format_write, driver
