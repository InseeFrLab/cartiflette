import subprocess


def mapshaper_convert_mercator(
    local_dir="temp",
    territory="reunion",
    file="COMMUNE",
    extension_initial="shp",
    format_intermediate="geojson",
    output_path=None,
):
    if output_path is None:
        output_path = f"{local_dir}/preprocessed"

    output_name = f"{output_path}/{territory}.{format_intermediate}"

    subprocess.run(
        (
            f"mapshaper {local_dir}/{territory}/COMMUNE.{extension_initial} name='COMMUNE' "
            f"-proj EPSG:4326 "
            f"-o {output_name} "
            f'format={format_intermediate} extension=".{format_intermediate}" singles'
        ),
        shell=True,
        check=True,
    )

    return output_name
