import argparse
import json
import logging
from cartiflette.pipeline import crossproduct_parameters_production

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

parser = argparse.ArgumentParser(description='Crossproduct Script')
parser.add_argument('--restrictfield', type=str, default=None, help='Field to restrict level-polygons')

def main():
    args = parser.parse_args()
    
    tempdf = crossproduct_parameters_production(
        croisement_filter_by_borders=croisement_decoupage_level,
        list_format=formats,
        years=years,
        crs_list=crs_list,
        sources=sources, simplifications=[0, 50])
    
    tempdf.columns = tempdf.columns.str.replace("_", "-")

    # Apply filtering if restrictfield is provided
    if args.restrictfield:
        tempdf = tempdf.loc[tempdf['level-polygons'] == args.restrictfield]

    output = tempdf.to_json(orient="records")
    parsed = json.loads(output)
    
    # Use logger to output the JSON
    logging.info(json.dumps(parsed))

if __name__ == "__main__":
    main()
