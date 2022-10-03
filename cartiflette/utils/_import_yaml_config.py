# Read YAML file
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
    with open(location, 'r', encoding="utf-8") as stream:
        dict_open_data = yaml.safe_load(stream)
    return dict_open_data

