import subprocess
import os

logical_conditions = {
    "DEPARTEMENT": {
        "ile de france": "['75', '92', '93', '94'].includes(INSEE_DEP)",
        "guadeloupe": "INSEE_DEP == '971'",
        "martinique": "INSEE_DEP == '972'",
        "guyane": "INSEE_DEP == '973'",
        "reunion": "INSEE_DEP == '974'",
        "mayotte": "INSEE_DEP == '976'",
        "zoom idf": 4,
    },
    "REGION": {
        "ile de france": "INSEE_REG == 11",
        "guadeloupe": "INSEE_REG == 1",
        "martinique": "INSEE_REG == 2",
        "guyane": "INSEE_REG == 3",
        "reunion": "INSEE_REG == 4",
        "mayotte": "INSEE_REG == 6",
        "zoom idf": 1.5,
    },
    "BASSIN_VIE": {
        "ile de france": "BV2012 == 75056",
        "guadeloupe": 'BV2012.startsWith("971")',
        "martinique": 'BV2012.startsWith("972")',
        "guyane": 'BV2012.startsWith("973")',
        "reunion": 'BV2012.startsWith("974")',
        "mayotte": 'BV2012.startsWith("976")',
        "zoom idf": 1.5,
    },
    "UNITE_URBAINE": {
        "ile de france": "UU2020 == '00851'",
        "guadeloupe": "UU2020.startsWith('9A')",
        "martinique": "UU2020.startsWith('9B')",
        "guyane": "UU2020.startsWith('9C')",
        "reunion": "UU2020.startsWith('9D')",
        "mayotte": "UU2020.startsWith('9F')",
        "zoom idf": 1.5,
    },
    "ZONE_EMPLOI": {
        "ile de france": 'ZE2020 == 1109',
        "guadeloupe": "ZE2020 >= 101 && ZE2020 <= 199",
        "martinique": 'ZE2020 >= 201 && ZE2020 <= 299',
        "guyane": 'ZE2020 >= 301 && ZE2020 <= 399',
        "reunion": 'ZE2020 >= 401 && ZE2020 <= 499',
        "mayotte": 'ZE2020 >= 601 && ZE2020 <= 699',
        "zoom idf": 1.5,
    },
    "AIRE_ATTRACTION_VILLES": {
        "ile de france": "AAV2020 == '001'",
        "guadeloupe": "bbox=-6880639.760944527,1785277.734007631,-6790707.017202182,1864381.5053494961",
        "martinique": 'bbox=-6815985.711078632,1618842.9696702233,-6769303.6899859235,1675227.3853840816',
        "guyane": "bbox=-6078313.094526156,235057.05702474713,-5746208.123095576,641016.7211362486",
        "reunion": 'bbox=6146675.557436854,-2438398.996947137,6215705.133130206,-2376601.891080389',
        "mayotte": 'bbox=5011418.778972076,-1460351.1566339568,5042772.003914668,-1418243.6428180535',
        "zoom idf": 1.5,

    }

}


def mapshaper_bring_closer(france_vector_path, level_agreg="DEPARTEMENT"):
    output_path = "temp/preprocessed_transformed/idf_combined.geojson"
    output_dir = os.path.dirname(output_path)

    logical_idf = logical_conditions[level_agreg]["ile de france"]
    logical_guadeloupe = logical_conditions[level_agreg]["guadeloupe"]
    logical_martinique = logical_conditions[level_agreg]["martinique"]
    logical_guyane = logical_conditions[level_agreg]["guyane"]
    logical_reunion = logical_conditions[level_agreg]["reunion"]
    logical_mayotte = logical_conditions[level_agreg]["mayotte"]
    zoom_idf = logical_conditions[level_agreg]["zoom idf"]

    idf_zoom = (
        f"mapshaper -i {france_vector_path} "
        f"-proj EPSG:3857 "
        f'-filter "{logical_idf}" '
        f"-affine shift=-650000,275000 scale={zoom_idf} "
        f"-o {output_dir}/idf_zoom.geojson"
    )

    temp_france = (
        f"mapshaper -i {france_vector_path} "
        f"-proj EPSG:3857 "
        f'-affine where="{logical_guadeloupe}" shift=6355000,3330000 scale=1.5 '
        f'-affine where="{logical_martinique}" shift=6480000,3505000 scale=1.5 '
        f'-affine where="{logical_guyane}" shift=5760000,4720000 scale=0.35 '
        f'-affine where="{logical_reunion}" shift=-6170000,7560000 scale=1.5 '
        f'-affine where="{logical_mayotte}" shift=-4885000,6590000 scale=1.5 '
        f"-o {output_dir}/temp_france.geojson "
    )

    subprocess.run(
        idf_zoom,
        shell=True,
        check=True,
    )

    subprocess.run(
        temp_france,
        shell=True,
        check=True,
    )

    cmd_combined = (
        f"mapshaper "
        f"{output_dir}/temp_france.geojson "
        f"{output_dir}/idf_zoom.geojson "
        f"snap combine-files "
        f'-proj wgs84 init="EPSG:3857" target=* '
        f"-rename-layers FRANCE,IDF "
        f"-merge-layers target=FRANCE,IDF force "
        f"-rename-layers FRANCE_TRANSFORMED "
        f"-o {output_dir}/idf_combined.geojson "
    )

    subprocess.run(
        cmd_combined,
        shell=True,
        check=True,
    )

    return output_path
