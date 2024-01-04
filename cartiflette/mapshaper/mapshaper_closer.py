import subprocess
import os


def mapshaper_bring_closer(
    france_vector_path
):

    output_path = "temp/preprocessed_transformed/idf_combined.geojson"
    output_dir = os.path.dirname(output_path)

    idf_zoom = (
        f"mapshaper -i {france_vector_path} "
        f"-proj EPSG:3857 "
        f"-filter \"['75', '92', '93', '94'].includes(INSEE_DEP)\" "
        f"-affine where=\"['75', '92', '93', '94'].includes(INSEE_DEP)\" shift=-650000,275000 scale=4 "
        f"-o {output_dir}/idf_zoom.geojson"
    )

    temp_france = (
        f"mapshaper -i {france_vector_path} "
        f"-proj EPSG:3857 "
        f"-affine where=\"INSEE_DEP == '971'\" shift=6355000,3330000 scale=1.5 "
        f"-affine where=\"INSEE_DEP == '972'\" shift=6480000,3505000 scale=1.5 "
        f"-affine where=\"INSEE_DEP == '973'\" shift=5760000,4720000 scale=0.35 "
        f"-affine where=\"INSEE_DEP == '974'\" shift=-6170000,7560000 scale=1.5 "
        f"-affine where=\"INSEE_DEP == '976'\" shift=-4885000,6590000 scale=1.5 "
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
        f"-proj wgs84 init=\"EPSG:3857\" target=* "
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
