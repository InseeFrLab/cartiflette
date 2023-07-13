# -*- coding: utf-8 -*-
"""
Created on Thu May 11 20:11:03 2023

@author: thomas.grandjean
"""
from typing import Dict, Any
import json


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




def update_json_md5(self, md5: str, fs) -> bool:
    "Mise à jour du json des md5"
    md5 = {
        self.provider: {
            self.dataset_family: {
                self.source: {self.territory: {str(self.year): md5}}
            }
        }
    }
    print(md5)
    path_filesystem = self.json_md5
    json_in_bucket = path_filesystem in fs.ls(
        path_filesystem.rsplit("/", maxsplit=1)[0]
    )
    try:
        if json_in_bucket:
            with fs.open(self.json_md5, "r") as f:
                all_md5 = json.load(f)
                all_md5 = deep_dict_update(all_md5, md5)
        else:
            all_md5 = md5
        with fs.open(self.json_md5, "w") as f:
            json.dump(all_md5, f)
        return True
    except Exception as e:
            logger.warning(e)
            logger.warning("md5 not written")
            return False
