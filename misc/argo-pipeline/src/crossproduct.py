import cartiflette.s3 as s3
import json
import sys

# parameters
formats = ["topojson"]
years = [2022]
crs_list = [4326, "official"]
sources = ["EXPRESS-COG-CARTO-TERRITOIRE"]

croisement_decoupage_level = {
    # structure -> niveau geo: [niveau decoupage macro],
    "REGION": ["FRANCE_ENTIERE"],
    "ARRONDISSEMENT_MUNICIPAL" : ['DEPARTEMENT'], 
    "COMMUNE_ARRONDISSEMENT": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "COMMUNE": ["DEPARTEMENT", "REGION", "FRANCE_ENTIERE"],
    "DEPARTEMENT": ["REGION", "FRANCE_ENTIERE"]
}


def main():
    tempdf = s3.crossproduct_parameters_production(
        croisement_filter_by_borders=croisement_decoupage_level,
        list_format=formats,
        years=years,
        crs_list=crs_list,
        sources=sources)
    output  = tempdf.to_json(orient="records")
    parsed = json.loads(output)
    json.dump(parsed, sys.stdout)

if __name__ == "__main__":
    main()