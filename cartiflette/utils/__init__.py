"""utils that help for other cartiflette packages"""

from ._import_yaml_config import (
    import_yaml_config,
    url_express_COG_territoire,
)
from .dict_correspondance import (
    dict_corresp_filter_by,
    create_format_standardized,
    create_format_driver,
    official_epsg_codes,
    DICT_CORRESP_ADMINEXPRESS,
)

from .keep_subset_geopandas import keep_subset_geopandas
from .hash import hash_file
from .dict_update import deep_dict_update

from .csv_magic import magic_csv_reader
from .create_path_bucket import create_path_bucket
from .standardize_inputs import standardize_inputs


__all__ = [
    "import_yaml_config",
    "url_express_COG_territoire",
    "dict_corresp_filter_by",
    "create_format_standardized",
    "create_format_driver",
    "keep_subset_geopandas",
    "official_epsg_codes",
    "hash_file",
    "deep_dict_update",
    "magic_csv_reader",
    "create_path_bucket",
    "standardize_inputs",
    "DICT_CORRESP_ADMINEXPRESS",
]
