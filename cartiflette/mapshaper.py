"""
Geographic processing with mapshaper (https://github.com/mbloch/mapshaper):
dissolve of communes into larger levels, DROM brought closer to metropolitan
France, reprojection and simplification.
"""

from __future__ import annotations

import logging
import os
import subprocess

logger = logging.getLogger(__name__)

# Field holding the code of each level. Zonings with a vintage in their field
# name (BASSIN_VIE -> BV2022...) are added at runtime from fields.json.
LEVEL_FIELDS = {
    "COMMUNE": "INSEE_COM",
    "COMMUNE_ARRONDISSEMENT": "INSEE_COG",
    "DEPARTEMENT": "INSEE_DEP",
    "REGION": "INSEE_REG",
    "TERRITOIRE": "AREA",
    "FRANCE_ENTIERE": "PAYS",
    "FRANCE_ENTIERE_DROM_RAPPROCHES": "PAYS",
}
LABEL_FIELDS = {
    "DEPARTEMENT": "LIBELLE_DEPARTEMENT",
    "REGION": "LIBELLE_REGION",
}

# Bounding boxes (EPSG:3857) of each territory
EXTENTS = {
    "metropole": "-572324.2901945524,5061666.243842439,1064224.7522608414,6638201.7541528195",
    "guadeloupe": "-6880639.760944527,1785277.734007631,-6790707.017202182,1864381.5053494961",
    "martinique": "-6815985.711078632,1618842.9696702233,-6769303.6899859235,1675227.3853840816",
    "guyane": "-6078313.094526156,235057.05702474713,-5746208.123095576,641016.7211362486",
    "reunion": "6146675.557436854,-2438398.996947137,6215705.133130206,-2376601.891080389",
    "mayotte": "5011418.778972076,-1460351.1566339568,5042772.003914668,-1418243.6428180535",
}
# Translation and scale bringing each DROM close to metropolitan France
DROM_AFFINE = {
    "guadeloupe": ("6355000,3330000", "1.5"),
    "martinique": ("6480000,3505000", "1.5"),
    "guyane": ("5760000,4720000", "0.35"),
    "reunion": ("-6170000,7560000", "1.5"),
    "mayotte": ("-4885000,6590000", "1.5"),
}
# Ile-de-France (or Paris' area for the zonings) is also displayed zoomed in
IDF_ZOOM = {
    "DEPARTEMENT": ("['75', '92', '93', '94'].includes(INSEE_DEP)", "4"),
    "REGION": ("INSEE_REG == '11'", "1.5"),
    "BASSIN_VIE": ("{field} == '75056'", "1.5"),
    "UNITE_URBAINE": ("{field} == '00851'", "1.5"),
    "ZONE_EMPLOI": ("{field} == '1109'", "1.5"),
    "AIRE_ATTRACTION_VILLES": ("{field} == '001'", "1.5"),
}
IDF_SHIFT = "-650000,275000"


def run(*args: str) -> None:
    cmd = ["mapshaper", *args]
    logger.debug(" ".join(cmd))
    subprocess.run(cmd, check=True)


def level_fields(zoning_fields: dict[str, str]) -> dict[str, str]:
    """Field of each level, including the zonings (e.g. BASSIN_VIE -> BV2022)."""
    return {**LEVEL_FIELDS, **zoning_fields}


def dissolve(
    input_path: str,
    output_path: str,
    level: str,
    keep_levels: list[str],
    fields: dict[str, str],
) -> str:
    """
    Dissolve communes into `level`, summing populations and keeping the
    fields of `level` and of each of `keep_levels` (and their labels).

    Communes of different territories are never merged: the dissolve is by
    the code of `level` and the territory (AREA). Otherwise a code spanning
    several territories (e.g. AAV2020 "000", communes outside any
    attraction area) makes one feature covering metropolitan France and the
    DROM, which `bring_drom_closer` cannot move.
    """
    keys = [fields[level], LEVEL_FIELDS["TERRITOIRE"]]
    levels = [level, *keep_levels]
    copy_fields = [fields[x] for x in levels]
    copy_fields += [LABEL_FIELDS[x] for x in levels if x in LABEL_FIELDS]
    copy_fields = [x for x in dict.fromkeys(copy_fields) if x not in keys]
    run(
        input_path,
        "-dissolve",
        ",".join(dict.fromkeys(keys)),
        "calc=POPULATION=sum(POPULATION)",
        f"copy-fields={','.join(copy_fields)}",
        "-o",
        output_path,
        "force",
    )
    return output_path


def bring_drom_closer(
    input_path: str,
    output_path: str,
    level: str,
    fields: dict[str, str],
) -> str:
    """
    Move the DROM next to metropolitan France and add a zoomed-in
    Ile-de-France, in a single layer (WGS84). The IDF zoom depends on the
    level of the polygons (departements for communes).
    """
    work_dir = os.path.dirname(output_path)
    idf_level = "DEPARTEMENT" if level.startswith("COMMUNE") else level
    idf_filter, idf_scale = IDF_ZOOM[idf_level]
    idf_filter = idf_filter.format(field=fields.get(idf_level, ""))

    parts = {
        "metropole": ["-filter", f"bbox={EXTENTS['metropole']}"],
        "idf": [
            "-filter",
            idf_filter,
            "-affine",
            f"shift={IDF_SHIFT}",
            f"scale={idf_scale}",
        ],
    }
    for territory, (shift, scale) in DROM_AFFINE.items():
        parts[territory] = [
            "-filter",
            f"bbox={EXTENTS[territory]}",
            "-affine",
            f"shift={shift}",
            f"scale={scale}",
        ]

    part_paths = []
    for name, commands in parts.items():
        part_path = os.path.join(work_dir, f"part_{name}.geojson")
        run("-i", input_path, "-proj", "EPSG:3857", *commands, "-o", part_path, "force")
        part_paths.append(part_path)

    run(
        "-i",
        *part_paths,
        "snap",
        "combine-files",
        "-proj",
        "wgs84",
        "init=EPSG:3857",
        "target=*",
        "-merge-layers",
        "target=*",
        "force",
        "-o",
        output_path,
        "force",
    )
    for part_path in part_paths:
        os.remove(part_path)
    return output_path


def _finalize_commands(crs: int, simplification: float, source_label: str) -> list:
    """Reprojection, simplification and SOURCE field."""
    simplify = ["-simplify", f"{simplification}%"] if simplification else []
    return [
        "-proj",
        f"EPSG:{crs}",
        *simplify,
        "-each",
        f"SOURCE='{source_label}'",
    ]


def finalize(
    input_path: str,
    output_path: str,
    crs: int,
    simplification: float,
    source_label: str,
) -> str:
    """Reproject and simplify `input_path` into a single GeoJSON."""
    run(
        input_path,
        "name=",
        *_finalize_commands(crs, simplification, source_label),
        "-o",
        output_path,
        "format=geojson",
        "force",
    )
    return output_path
