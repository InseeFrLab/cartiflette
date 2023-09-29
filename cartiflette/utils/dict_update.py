# -*- coding: utf-8 -*-
from typing import Dict, Any


def deep_dict_update(
    mapping: Dict[Any, Any], *updating_mappings: Dict[Any, Any]
) -> Dict[Any, Any]:
    """
    https://stackoverflow.com/questions/3232943/#answer-68557484
    Recursive update of a nested dictionary

    Parameters
    ----------
    mapping : Dict[KeyType, Any]
        initial dictionary
    *updating_mappings : Dict[KeyType, Any]
        update to set into mapping

    Returns
    -------
    Dict[KeyType, Any]
        new (udpated) dictionary

    """

    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if (
                k in updated_mapping
                and isinstance(updated_mapping[k], dict)
                and isinstance(v, dict)
            ):
                updated_mapping[k] = deep_dict_update(updated_mapping[k], v)
            else:
                updated_mapping[k] = v
    return updated_mapping
