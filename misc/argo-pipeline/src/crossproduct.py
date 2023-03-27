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
    #output  = tempdf.to_json(orient="records")
    #parsed = json.loads(output)
    #json.dump(parsed, sys.stdout)
    json.dump([i for i in range(20, 31)], sys.stdout)
    #print('[{"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "REGION", "filter_by": "FRANCE_ENTIERE"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "REGION", "filter_by": "FRANCE_ENTIERE"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "ARRONDISSEMENT_MUNICIPAL", "filter_by": "DEPARTEMENT"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "ARRONDISSEMENT_MUNICIPAL", "filter_by": "DEPARTEMENT"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE_ARRONDISSEMENT", "filter_by": "DEPARTEMENT"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE_ARRONDISSEMENT", "filter_by": "DEPARTEMENT"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE_ARRONDISSEMENT", "filter_by": "REGION"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE_ARRONDISSEMENT", "filter_by": "REGION"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE_ARRONDISSEMENT", "filter_by": "FRANCE_ENTIERE"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE_ARRONDISSEMENT", "filter_by": "FRANCE_ENTIERE"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE", "filter_by": "DEPARTEMENT"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE", "filter_by": "DEPARTEMENT"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE", "filter_by": "REGION"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE", "filter_by": "REGION"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE", "filter_by": "FRANCE_ENTIERE"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "COMMUNE", "filter_by": "FRANCE_ENTIERE"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "DEPARTEMENT", "filter_by": "REGION"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "DEPARTEMENT", "filter_by": "REGION"}, {"format": "topojson", "year": 2022, "crs": 4326, "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "DEPARTEMENT", "filter_by": "FRANCE_ENTIERE"}, {"format": "topojson", "year": 2022, "crs": "official", "source": "EXPRESS-COG-CARTO-TERRITOIRE", "borders": "DEPARTEMENT", "filter_by": "FRANCE_ENTIERE"}]')

if __name__ == "__main__":
    main()