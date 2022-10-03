# Read YAML file
def import_yaml_config(location: str = "resources/sources.yaml") -> dict:
    """
    Import data sources list from YAML file

    Args:
        location (str, optional): YAML file location.
            Defaults to "resources/sources.yaml".

    Returns:
        dict: hierarchical dict where, for each source, we have some relevent information
            for imports
    """
    with open(location, 'r', encoding="utf-8") as stream:
        dict_open_data = yaml.safe_load(stream)
    return dict_open_data

