import logging
import os

from cartiflette.mapshaper.utils import run

logger = logging.getLogger(__name__)

# TODO : TOM (St-Martin, St-Barthelemy, St-Pierre-et-Miquelon)

logical_conditions = {
    "EMPRISES": {
        # left, bottom, right, top (epsg=3857)
        "ile de france": "IDF=1",
        "metropole": "bbox=-572324.2901945524,5061666.243842439,1064224.7522608414,6638201.7541528195",
        "guadeloupe": "bbox=-6880639.760944527,1785277.734007631,-6790707.017202182,1864381.5053494961",
        "martinique": "bbox=-6815985.711078632,1618842.9696702233,-6769303.6899859235,1675227.3853840816",
        "guyane": "bbox=-6078313.094526156,235057.05702474713,-5746208.123095576,641016.7211362486",
        "reunion": "bbox=6146675.557436854,-2438398.996947137,6215705.133130206,-2376601.891080389",
        "mayotte": "bbox=5011418.778972076,-1460351.1566339568,5042772.003914668,-1418243.6428180535",
        "saint-martin": "bbox=-7034906.766337046, 2038329.0872462029, -7009537.630813715, 2056865.7060235194",
        "saint-pierre-et-miquelon": "bbox=-6298822.299318486, 5894013.594517256, -6239181.296921183, 5973004.907786214",
        "saint-barthelemy": "bbox=-7003557.376380256, 2018598.440800959, -6985037.106437805, 2033965.5078367123",
    },
    "REGION": 1.5,
    "BASSIN_VIE": 1.5,
    "UNITE_URBAINE": 1.5,
    "ZONE_EMPLOI": 1.5,
    "AIRE_ATTRACTION_VILLES": 1.2,
    "DEPARTEMENT": 4,
}

shift = {
    # X, Y shift
    "guadeloupe": "6355000,3330000",
    "martinique": "6480000,3505000",
    "guyane": "5760000,4720000",
    "reunion": "-6170000,7560000",
    "mayotte": "-4885000,6590000",
    "saint-martin": "5690000,-900000",
    "saint-pierre-et-miquelon": "2880000,-2910000",
    "saint-barthelemy": "5670000,-730000",
}

scale = {
    "guadeloupe": "1.5",
    "martinique": "1.5",
    "guyane": "0.35",
    "reunion": "1.5",
    "mayotte": "1.5",
    "saint-martin": "2.5",
    "saint-pierre-et-miquelon": "2",
    "saint-barthelemy": "2.5",
}


def mapshaper_bring_closer(
    input_file: str,
    output_dir: str = "temp",
    output_name: str = "output",
    output_format: str = "geojson",
    level_agreg: str = "DEPARTEMENT",
):
    """
    Bring DROM closer and zoom over IDF.

    Parameters
    ----------
    input_file : str
        Path to the input file.
    output_dir : str
        Directory to store the output file. The default is "temp"
    output_name : str, optional
        The path to write the file to (without extension).
        The default is "concatenated"
    output_format : str, optional
        The format to write the outputfile. The default is "geojson".
    level_agreg : str, optional
        Desired aggregation configuration. The default is "DEPARTEMENT".

    Returns
    -------
    str
        Local path to the output file

    """

    try:
        os.makedirs(output_dir)
    except FileExistsError:
        pass

    logical_idf = logical_conditions["EMPRISES"]["ile de france"]
    zoom_idf = logical_conditions.get(level_agreg, 1.5)
    logical_metropole = logical_conditions["EMPRISES"]["metropole"]

    try:
        idf_zoom = (
            f"mapshaper -i {input_file} "
            f"-proj EPSG:3857 "
            f'-filter "{logical_idf}" '
            f"-affine shift=-650000,275000 scale={zoom_idf} "
            f"-o {output_dir}/idf_zoom.{output_format}"
        )

        france_metropolitaine = (
            f"mapshaper -i {input_file} "
            f"-proj EPSG:3857 "
            f'-filter "{logical_metropole}" '
            f"-o {output_dir}/metropole.{output_format}"
        )

        run(idf_zoom)

        run(france_metropolitaine)

        for region, shift_value in shift.items():
            logger.info("Processing %s", region)
            cmd = (
                f"mapshaper -i {input_file} "
                f"-proj EPSG:3857 "
                f'-filter "{logical_conditions["EMPRISES"][region]}" '
                f"-affine shift={shift_value} scale={scale[region]} "
                f"-o {output_dir}/{region}.{output_format}"
            )
            run(cmd)

        # fix_geo = "fix-geometry" if output_format == "topojson" else ""

        output = f"{output_dir}/{output_name}.{output_format}"
        cmd_combined = (
            f"mapshaper "
            f"{output_dir}/metropole.{output_format} "
            f"{output_dir}/idf_zoom.{output_format} "
            f"{output_dir}/guadeloupe.{output_format} "
            f"{output_dir}/martinique.{output_format} "
            f"{output_dir}/guyane.{output_format} "
            f"{output_dir}/reunion.{output_format} "
            f"{output_dir}/mayotte.{output_format} "
            f"snap combine-files "
            f'-proj wgs84 init="EPSG:3857" target=* '
            f"-rename-layers FRANCE,IDF,GDP,MTQ,GUY,REU,MAY "
            f"-merge-layers target=FRANCE,IDF,GDP,MTQ,GUY,REU,MAY force "
            f"-rename-layers FRANCE_TRANSFORMED "
            f"-o {output} "
            # f"{fix_geo}"
        )

        run(cmd_combined)
    except Exception:
        raise

    finally:
        for tempfile in [
            "metropole",
            "idf_zoom",
            "guadeloupe",
            "martinique",
            "guyane",
            "reunion",
            "mayotte",
        ]:
            try:
                os.unlink(f"{output_dir}/{tempfile}.{output_format}")
            except FileNotFoundError:
                pass

    return output
