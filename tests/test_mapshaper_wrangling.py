import unittest
import tempfile
import os
import glob
import shutil
from shapely.geometry import Point
import pandas as pd
import geopandas as gpd

# Import the functions to be tested
from cartiflette.mapshaper.mapshaper_wrangling import mapshaper_enrich, mapshaper_split


class TestMapshaperWrangling(unittest.TestCase):
    def setUp(self):
        # Create temporary files for testing
        self.input_shapefile = tempfile.NamedTemporaryFile(suffix=".geojson", delete=False)
        gdf_shapefile = gpd.GeoDataFrame(
            {
                "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
                "INSEE_COM": ["1", "2", "3"],
                "INSEE_CAN": ["A", "B", "C"],
                "INSEE_ARR": ["D", "E", "F"],
                "SIREN_EPCI": ["G", "H", "I"],
                "INSEE_DEP": ["J", "K", "L"],
                "INSEE_REG": ["M", "N", "O"],
                "NOM_M": ["P", "Q", "R"],
            },
            crs="EPSG:4326",
        )
        gdf_shapefile.to_file(self.input_shapefile.name, driver="GeoJSON")

        self.input_csv = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        df_csv = pd.DataFrame(
            {
                "CODGEO": ["1", "2", "3"],
                "DEP": ["A", "B", "C"],
                "REG": ["M", "N", "O"],
                "REGION": ["X", "Y", "Z"],
            }
        )
        df_csv.to_csv(self.input_csv.name, index=False)

        self.input_geojson = tempfile.NamedTemporaryFile(suffix=".geojson", delete=False)
        gdf_geojson = gpd.GeoDataFrame(
            {
                "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
                "DEPARTEMENT": ["A", "B", "A"],
            },
            crs="EPSG:4326",
        )
        gdf_geojson.to_file(self.input_geojson.name, driver="GeoJSON")

    def tearDown(self):
        # Clean up: remove the temporary files
        os.remove(self.input_shapefile.name)
        os.remove(self.input_csv.name)
        os.remove(self.input_geojson.name)

    def test_mapshaper_enrich(self):
        # Set up the parameters for the function
        output_path = tempfile.TemporaryDirectory().name

        try:
            # Call the function to be tested
            mapshaper_enrich(
                local_dir=os.path.dirname(self.input_shapefile.name),
                filename_initial=os.path.basename(self.input_shapefile.name).replace(
                    ".geojson", ""
                ),
                extension_initial="geojson",
                metadata_file=self.input_csv.name,
                output_path=f"{output_path}/enriched.geojson",
                dict_corresp={"FRANCE_ENTIERE": "Country"},
            )

            # Check if the output GeoJSON file exists
            self.assertTrue(os.path.exists(f"{output_path}/enriched.geojson"))

            # Count number of features
            gdf_output = gpd.read_file(f"{output_path}/enriched.geojson")
            self.assertEqual(len(gdf_output), 3)

            # Check if the expected fields are present
            expected_fields = ["geometry", "INSEE_COM", "INSEE_DEP", "INSEE_REG"]
            for field in expected_fields:
                self.assertIn(field, gdf_output.columns)

            # Additional checks based on your specific requirements

        finally:
            # Clean up: remove the temporary output directory
            shutil.rmtree(output_path)

    def test_mapshaper_split(self):
        # Set up the parameters for the function
        layer_name = "test_layer"
        split_variable = "DEPARTEMENT"
        output_path = tempfile.TemporaryDirectory().name

        try:
            # Call the function to be tested
            mapshaper_split(
                input_file=self.input_geojson.name,
                layer_name=layer_name,
                split_variable=split_variable,
                output_path=f"{output_path}/new.geojson",
            )

            # Check if the output GeoJSON file exists
            self.assertTrue(os.path.exists(output_path))

            # Count number of files
            files = glob.glob(f"{output_path}/*.geojson")
            print(files)
            file_count = len(files)
            self.assertEqual(
                file_count, 2
            )  # Check if the number of features is as expected

            # Count number of features
            gdf_output = pd.concat([gpd.read_file(tempgpd) for tempgpd in files])
            self.assertEqual(len(gdf_output), 3)

        finally:
            # Clean up: remove the temporary output
            shutil.rmtree(output_path)


if __name__ == "__main__":
    unittest.main()
