import argparse
import pandas as pd

# Initialize ArgumentParser
parser = argparse.ArgumentParser(description="Run Cartiflette pipeline script.")
parser.add_argument("-p", "--path", help="Path within bucket", default="tmp/tagc.csv")

# Parse arguments
args = parser.parse_args()

path_tagc = args.path

if __name__ == '__main__':
    print(pd.read_csv(path_tagc).head())