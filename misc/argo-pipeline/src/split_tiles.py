import argparse
from cartiflette import PATH_WITHIN_BUCKET
from cartiflette.pipeline import mapshaperize_split_from_s3

parser = argparse.ArgumentParser(description='Process command line arguments.')

# Define the arguments with their default values
parser.add_argument('--path', type=str, default=PATH_WITHIN_BUCKET, help='Path in bucket')
parser.add_argument('--format_output', type=str, default='geojson', help='Output format')
parser.add_argument('--year', type=int, default=2022, help='Year for the data')
parser.add_argument('--crs', type=int, default=4326, help='Coordinate Reference System')
parser.add_argument('--source', type=str, default='EXPRESS-COG-CARTO-TERRITOIRE', help='Data source')
parser.add_argument('--simplification', type=float, default=0, help='Simplification level')
parser.add_argument('--level_polygons', type=str, default='COMMUNE', help='Level of polygons')
parser.add_argument('--filter_by', type=str, default='DEPARTEMENT', help='Splitting criteria')

# Parse the arguments
args = parser.parse_args()

# Create a dictionary from the parsed arguments
args_dict = {
    'path_within_bucket': args.path_within_bucket,
    'format_output': args.format_output,
    'year': args.year,
    'crs': args.crs,
    'source': args.source,
    'simplification': args.simplification,
    'level_polygons': args.level_polygons,
    'filter_by': args.filter_by
}


def main(args_dict):
    mapshaperize_split_from_s3(args_dict)


if __name__ == "__main__":
    main()
