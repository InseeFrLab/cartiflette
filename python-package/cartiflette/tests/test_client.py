#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 11:15:09 2024

Test cartiflette client
"""

import geopandas as gpd

from cartiflette import carti_download


def test_carti_download():
    dataset_topojson = carti_download(
        values=["France"],
        crs=4326,
        borders="DEPARTEMENT",
        vectorfile_format="topojson",
        simplification=50,
        filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        year=2022,
    )
    dataset_geojson = carti_download(
        values=["France"],
        crs=4326,
        borders="DEPARTEMENT",
        vectorfile_format="geojson",
        simplification=50,
        filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
        source="EXPRESS-COG-CARTO-TERRITOIRE",
        year=2022,
    )
    assert isinstance(dataset_topojson, gpd.GeoDataFrame)
    assert isinstance(dataset_geojson, gpd.GeoDataFrame)
