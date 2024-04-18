import subprocess
import os

logical_conditions = {
    "EMPRISES": {
        "metropole": "bbox=-572324.2901945524,5061666.243842439,1064224.7522608414,6638201.7541528195",
        "guadeloupe": "bbox=-6880639.760944527,1785277.734007631,-6790707.017202182,1864381.5053494961",
        "martinique": 'bbox=-6815985.711078632,1618842.9696702233,-6769303.6899859235,1675227.3853840816',
        "guyane": "bbox=-6078313.094526156,235057.05702474713,-5746208.123095576,641016.7211362486",
        "reunion": 'bbox=6146675.557436854,-2438398.996947137,6215705.133130206,-2376601.891080389',
        "mayotte": 'bbox=5011418.778972076,-1460351.1566339568,5042772.003914668,-1418243.6428180535'
    },
    "DEPARTEMENT": {
        "ile de france": "['75', '92', '93', '94'].includes(INSEE_DEP)",
        "zoom idf": 4,
    },
    "REGION": {
        "ile de france": "INSEE_REG == 11",
        "zoom idf": 1.5
    },
    "BASSIN_VIE": {
        "ile de france": "BV2012 == 75056",
        "zoom idf": 1.5
    },
    "UNITE_URBAINE": {
        "ile de france": "UU2020 == '00851'",
        "zoom idf": 1.5
    },
    "ZONE_EMPLOI": {
        "ile de france": 'ZE2020 == 1109',
        "zoom idf": 1.5
    },
    "AIRE_ATTRACTION_VILLES": {
        "ile de france": "AAV2020 == '001'",
        "zoom idf": 1.5
    }

}

shift = {
    'guadeloupe': '6355000,3330000',
    'martinique': '6480000,3505000',
    'guyane': '5760000,4720000',
    'reunion': '-6170000,7560000',
    'mayotte': '-4885000,6590000',
}

scale = {
    'guadeloupe': '1.5',
    'martinique': '1.5',
    'guyane': '0.35',
    'reunion': '1.5',
    'mayotte': '1.5'
}


def mapshaper_bring_closer(
    france_vector_path="temp.geojson",
    level_agreg="DEPARTEMENT"
    ):

    output_path = "temp/preprocessed_transformed/idf_combined.geojson"
    output_dir = os.path.dirname(output_path)

    logical_idf = logical_conditions[level_agreg]["ile de france"]
    zoom_idf = logical_conditions[level_agreg]["zoom idf"]
    logical_metropole = logical_conditions["EMPRISES"]["metropole"]

    idf_zoom = (
        f"mapshaper -i {france_vector_path} "
        f"-proj EPSG:3857 "
        f'-filter "{logical_idf}" '
        f"-affine shift=-650000,275000 scale={zoom_idf} "
        f"-o {output_dir}/idf_zoom.geojson"
    )

    france_metropolitaine = (
        f"mapshaper -i {france_vector_path} "
        f"-proj EPSG:3857 "
        f'-filter "{logical_metropole}" '
        f"-o {output_dir}/metropole.geojson"
    )

    subprocess.run(
        idf_zoom,
        shell=True,
        check=True,
    )

    subprocess.run(
        france_metropolitaine,
        shell=True,
        check=True,
    )

    for region, shift_value in shift.items():
        print(f"Processing {region}")
        cmd = (
            f"mapshaper -i {france_vector_path} "
            f"-proj EPSG:3857 "
            f'-filter "{logical_conditions["EMPRISES"][region]}" '
            f'-affine shift={shift_value} scale={scale[region]} '
            f"-o {output_dir}/{region}.geojson"
        )
        subprocess.run(
            cmd,
            shell=True,
            check=True,
        )

    cmd_combined = (
        f"mapshaper "
        f"{output_dir}/metropole.geojson "
        f"{output_dir}/idf_zoom.geojson "
        f"{output_dir}/guadeloupe.geojson "
        f"{output_dir}/martinique.geojson "
        f"{output_dir}/guyane.geojson "
        f"{output_dir}/reunion.geojson "
        f"{output_dir}/mayotte.geojson "
        f"snap combine-files "
        f'-proj wgs84 init="EPSG:3857" target=* '
        f"-rename-layers FRANCE,IDF,GDP,MTQ,GUY,REU,MAY "
        f"-merge-layers target=FRANCE,IDF,GDP,MTQ,GUY,REU,MAY force "
        f"-rename-layers FRANCE_TRANSFORMED "
        f"-o {output_dir}/idf_combined.geojson "
    )

    subprocess.run(
        cmd_combined,
        shell=True,
        check=True,
    )

    return f"{output_dir}/idf_combined.geojson"