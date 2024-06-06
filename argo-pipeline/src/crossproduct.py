import json
import argparse
from cartiflette.pipeline import crossproduct_parameters_production

parser = argparse.ArgumentParser(description="Crossproduct Script")
parser.add_argument(
    "--restrictfield", type=str, default=None, help="Field to restrict level-polygons"
)


# parameters
formats = ["topojson", "geojson"]
years = [2022]
crs_list = [4326]
sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]

croisement_decoupage_level = {
    # structure -> niveau geo: [niveau decoupage macro],
    "COMMUNE": [
        "BASSIN_VIE",
        "ZONE_EMPLOI",
        "UNITE_URBAINE",
        "AIRE_ATTRACTION_VILLES",  # zonages d'études
        "DEPARTEMENT",
        "REGION",  # zonages administratifs
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "DEPARTEMENT": [
        "REGION",
        "TERRITOIRE",
        "FRANCE_ENTIERE",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
    ],
    "REGION": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "BASSIN_VIE": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "ZONE_EMPLOI": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "UNITE_URBAINE": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "AIRE_ATTRACTION_VILLES": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
}

args = parser.parse_args()


def main():
    tempdf = crossproduct_parameters_production(
        croisement_filter_by_borders=croisement_decoupage_level,
        list_format=formats,
        years=years,
        crs_list=crs_list,
        sources=sources,
        simplifications=[0, 50],
    )
    tempdf.columns = tempdf.columns.str.replace("_", "-")

    # Apply filtering if restrictfield is provided
    if args.restrictfield:
        tempdf = tempdf.loc[tempdf["level-polygons"] == args.restrictfield]

    output = tempdf.to_json(orient="records")
    parsed = json.loads(output)
    print(json.dumps(parsed))


if __name__ == "__main__":
    main()
