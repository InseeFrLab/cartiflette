import unittest

from cartiflette import carti_download
from cartiflette.config import PATH_WITHIN_BUCKET


class TestDownloadFromCartiflette(unittest.TestCase):
    def test_carti_download_single_value(self):
        # Call the function to be tested
        ile_de_france = carti_download(
            values="11",
            crs=4326,
            borders="DEPARTEMENT",
            vectorfile_format="topojson",
            filter_by="REGION",
            source="EXPRESS-COG-CARTO-TERRITOIRE",
            year=2022,
            path_within_bucket=PATH_WITHIN_BUCKET,
        )

        print(ile_de_france)
        # Check the properties of the resulting GeoPandas DataFrame
        self.assertEqual(len(ile_de_france), 8)  # Check the number of rows

        # Check the unique values in the 'INSEE_DEP' column
        unique_insee_dep = ile_de_france["INSEE_DEP"].unique()
        expected_unique_values = [
            "75",
            "77",
            "78",
            "91",
            "92",
            "93",
            "94",
            "95",
        ]
        self.assertCountEqual(unique_insee_dep, expected_unique_values)

    def test_carti_download_multi_values(self):
        # Call the function to be tested with multiple values
        ile_de_france_multi = carti_download(
            values=["11", "32", "44"],
            crs=4326,
            borders="DEPARTEMENT",
            vectorfile_format="topojson",
            filter_by="REGION",
            source="EXPRESS-COG-CARTO-TERRITOIRE",
            year=2022,
            path_within_bucket=PATH_WITHIN_BUCKET,
        )

        # Check the properties of the resulting GeoPandas DataFrame for multiple values
        self.assertEqual(
            len(ile_de_france_multi), 23
        )  # Check the number of rows

        # Check the unique values in the 'INSEE_REG' column
        unique_insee_reg = ile_de_france_multi["INSEE_REG"].unique()
        expected_unique_values_multi = [11, 32, 44]
        self.assertCountEqual(unique_insee_reg, expected_unique_values_multi)

    def test_carti_download_commune_arrondissement(self):
        # Call the function to be tested
        ile_de_france_commune_arrondissement = carti_download(
            values=["75"],
            crs=4326,
            borders="COMMUNE_ARRONDISSEMENT",
            vectorfile_format="topojson",
            filter_by="DEPARTEMENT",
            source="EXPRESS-COG-CARTO-TERRITOIRE",
            year=2022,
            path_within_bucket=PATH_WITHIN_BUCKET,
        )

        # Check the properties of the resulting GeoPandas DataFrame for commune arrondissement
        self.assertEqual(
            len(ile_de_france_commune_arrondissement), 20
        )  # Check the number of rows

        # Check unique values in specific columns
        unique_insee_com = ile_de_france_commune_arrondissement[
            "INSEE_COM"
        ].unique()
        self.assertCountEqual(unique_insee_com, ["75056"])

        unique_statut = ile_de_france_commune_arrondissement[
            "STATUT"
        ].unique()
        self.assertCountEqual(unique_statut, ["Arrondissement municipal"])

        unique_insee_cog = ile_de_france_commune_arrondissement[
            "INSEE_COG"
        ].unique()
        expected_insee_cog_values = [str(i) for i in range(75101, 75121)]
        self.assertCountEqual(unique_insee_cog, expected_insee_cog_values)


if __name__ == "__main__":
    unittest.main()
