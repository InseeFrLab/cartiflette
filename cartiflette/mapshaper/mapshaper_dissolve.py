#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess
from typing import List


def mapshaper_dissolve(
    file_in: str,
    file_out: str,
    by: str,
    copy_fields: List[str] = None,
    calc: List[str] = None,
    format_output: str = "geojson",
):
    """
    Dissolve geometries

    Dissolve geometries on field `by`, keeping fields `copy_fields`. Other
    fields should be computaded using javascript functions with `calc`
    argument.


    Parameters
    ----------
    by : str
        Field used to dissolve
    calc : Listr[str], optional
        Fields on which computed should be operated, describing valid js
        functions. For instance ["POPULATION=sum(POPULATION)"]. The default
        is None.
    copy_fields : List[str], optional
        Copies values from the first feature in each group of dissolved
        features. The default is None.
    format_output : str, optional
        Output format. The default is geojson

    Returns
    -------
    None.

    """
    cmd = (
        f"mapshaper {file_in} "
        f"name='by' "
        "-proj EPSG:4326 "
        f"-dissolve {by} "
    )
    if calc:
        calc = ",".join(calc)
        cmd += f"calc='{calc}' "
    if copy_fields:
        cmd += "copy-fields=" + ",".join(copy_fields)

    cmd += f" -o {file_out} force"

    subprocess.run(
        cmd,
        shell=True,
        check=True,
        text=True,
    )
