"""
Import sources from YAML
"""
# Read YAML file
from datetime import date
import os
import yaml

config_file = os.path.join(os.path.dirname(__file__), "sources.yaml")


def import_yaml_config(location: str = config_file) -> dict:
    """
    Import data sources list from YAML file

    Args:
        location (str, optional): YAML file location.
            Defaults to "resources/sources.yaml".

    Returns:
        dict: hierarchical dict where, for each source,
            we have some relevent information
            for imports
    """
    with open(location, "r", encoding="utf-8") as stream:
        dict_open_data = yaml.safe_load(stream)
    return dict_open_data


def url_express_COG_territoire(
    year: int = None, provider: str = "IGN", territoire: str = "metropole"
):
    # from cartiflette.utils import import_yaml_config

    if not year:
        year = date.today().year

    yaml = import_yaml_config()
    source = yaml[provider]["ADMINEXPRESS"]["EXPRESS-COG-TERRITOIRE"]

    # RETRIEVING FROM YAML
    territory = source["territory"][territoire]
    date_yaml = source[year]["date"]
    prefix = source[year]["prefix"]
    version = source[year]["version"]
    structure = source[year]["structure"]
    url_prefix = source[year]["url_prefix"]

    # REFORMATING
    url = structure.format(
        url_prefix=url_prefix,
        date=date_yaml,
        prefix=prefix,
        version=version,
        territory=territory,
    )

    return url
