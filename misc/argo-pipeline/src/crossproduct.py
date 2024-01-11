from cartiflette.pipeline import crossproduct_parameters_production
import json

# parameters
formats = ["topojson", "geojson"]
years = [2022]
crs_list = [4326]
sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]

croisement_decoupage_level = {
    # structure -> niveau geo: [niveau decoupage macro],
    "COMMUNE": [
        "BASSIN_VIE", "ZONE_EMPLOI", "UNITE_URBAINE", "AIRE_ATTRACTION_VILLES",  # zonages d'études
        "DEPARTEMENT", "REGION",  # zonages administratifs
        "TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "DEPARTEMENT": ["REGION", "TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "REGION": ["TERRITOIRE", "FRANCE_ENTIERE", "FRANCE_ENTIERE_DROM_RAPPROCHES"],
    "BASSIN_VIE": ["TERRITOIRE", "FRANCE_ENTIERE"],
    "ZONE_EMPLOI": ["TERRITOIRE", "FRANCE_ENTIERE"],
    "UNITE_URBAINE": ["TERRITOIRE", "FRANCE_ENTIERE"],
    "AIRE_ATTRACTION_VILLES": ["TERRITOIRE", "FRANCE_ENTIERE"]
}


def main():
    tempdf = crossproduct_parameters_production(
        croisement_filter_by_borders=croisement_decoupage_level,
        list_format=formats,
        years=years,
        crs_list=crs_list,
        sources=sources, simplifications=[0, 50])
    tempdf.columns = tempdf.columns.str.replace("_", "-")
    output = tempdf.to_json(orient="records")
    parsed = json.loads(output)
    print(json.dumps(parsed))

if __name__ == "__main__":
    main()