"""A simple API to expose cartiflette files"""
import typing
from fastapi import FastAPI, Response

from cartiflette.api import download_from_cartiflette_inner
from cartiflette.config import PATH_WITHIN_BUCKET

app = FastAPI(
    title="API de récupération des fonds de carte avec <code>cartiflette</code>",
    description='<br><br><img src="https://github.com/InseeFrLab/cartiflette/raw/main/cartiflette.png" width="200">',
)


@app.get("/", tags=["Welcome"])
def show_welcome_page():
    """
    Show welcome page with model name and version.
    """

    return {
        "Message": "API cartiflette",
        "Documentation": "https://github.com/InseeFrLab/cartiflette",
    }


@app.get("/json", tags=["Output a JSON object"])
def download_from_cartiflette_api(
    values: typing.List[typing.Union[str, int, float]] = "11",
    borders: str = "DEPARTEMENT",
    filter_by: str = "REGION",
    simplification: typing.Union[str, int, float] = None,
) -> str:
    """ """

    geojsons = download_from_cartiflette_inner(
        values=values,
        borders=borders,
        filter_by=filter_by,
        territory="metropole",
        vectorfile_format="topojson",
        year=2022,
        crs=4326,
        simplification=simplification,
        provider="IGN",
        dataset_family="ADMINEXPRESS",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        return_as_json=False,
        path_within_bucket=PATH_WITHIN_BUCKET,
    )

    geojson_dict = geojsons.to_json()

    return Response(geojson_dict, media_type="application/json")
