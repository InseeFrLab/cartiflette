"""utils that help for other cartiflette packages"""

from cartiflette.utils._import_yaml_config import (
    import_yaml_config,
    url_express_COG_territoire,
)
from cartiflette.utils.dict_correspondance import (
    dict_corresp_filter_by,
    create_format_standardized,
    create_format_driver,
    official_epsg_codes,
)

from cartiflette.utils.keep_subset_geopandas import keep_subset_geopandas
from cartiflette.utils.hash import hash_file
from cartiflette.utils.dict_update import deep_dict_update

from cartiflette.utils.csv_magic import magic_csv_reader


# from cartiflette.utils.vectorfile_format_config import (
#     _vectorfile_format_config,
# )
# from cartiflette.utils.s3_paths import _vectorfile_path

from cartiflette.utils.create_path_bucket import create_path_bucket

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
]
