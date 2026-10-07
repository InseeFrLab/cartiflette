"""
Integration tests: the classic uses shown on the cartiflette website
(https://github.com/InseeFrLab/cartiflette-website, use-case/ and the
interactive home page), run against published files, read-only.

Deselected by default, run them with:

    uv run pytest -m integration

The new vintages are read from CARTIFLETTE_TEST_PATH (default "test/v0.3.0",
the test location of the pipeline; "production" to check a publication) for
the years in CARTIFLETTE_TEST_YEARS (default "2022,2023,2024,2025,2026"). The
2022 GeoJSON files are also read from production, to check that the files
published by the former pipeline are still readable.

With CARTIFLETTE_API_URL (e.g. "http://localhost:8000", see api/), the same
use cases read GeoJSON from the API (/v1/geojson) instead of the files.
"""

import gzip
import io
import os
import urllib.parse
import urllib.request
import warnings

import duckdb
import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from cartiflette import carti_download

pytestmark = pytest.mark.integration

TEST_PATH = os.environ.get("CARTIFLETTE_TEST_PATH", "test/v0.3.0")
YEARS = [
    int(y)
    for y in os.environ.get("CARTIFLETTE_TEST_YEARS", "2022,2023,2024,2025,2026").split(
        ","
    )
]
LEGACY_YEAR = 2022

# (year, path_within_bucket): new files, then legacy production files
LOCATIONS = [(year, TEST_PATH) for year in YEARS] + [(LEGACY_YEAR, "production")]
NEW_LOCATIONS = [(year, TEST_PATH) for year in YEARS]

API_URL = os.environ.get("CARTIFLETTE_API_URL")
skip_with_api = pytest.mark.skipif(
    bool(API_URL), reason="option of the Python client, not of the API"
)

PETITE_COURONNE = ["75", "92", "93", "94"]
DROM = {"971", "972", "973", "974", "976"}


def has_zoom_duplicates(year, path):
    """
    Files of the former pipeline (2022 GeoJSON in production): the
    Ile-de-France zoom of the DROM rapproches layout is a second row of the
    same feature. One multipolygon per feature since pipeline 0.4.0.
    """
    return (year, path) == (LEGACY_YEAR, "production")


def download(year, path, **kwargs):
    kwargs.setdefault("crs", 4326)
    kwargs.setdefault("simplification", 50)
    if API_URL:
        return download_api(year, path, **kwargs)
    return carti_download(year=year, path_within_bucket=path, **kwargs)


def _read_geojson_url(url):
    request = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    with urllib.request.urlopen(request) as response:
        content = response.read()
        if response.headers.get("Content-Encoding") == "gzip":
            content = gzip.decompress(content)
    return gpd.read_file(io.BytesIO(content))


def download_api(
    year, path, values, borders, filter_by, crs, simplification, where=None, **_format
):
    """Same polygons as `carti_download`, as GeoJSON from the API."""
    query = urllib.parse.urlencode(
        {
            "year": year,
            "borders": borders,
            "filter_by": filter_by,
            "values": [values] if isinstance(values, (str, int)) else values,
            "crs": crs,
            "simplification": simplification,
            "path_within_bucket": path,
            **({"where": where} if where else {}),
        },
        doseq=True,
    )
    # Without GeoParquet (2022), redirected to the file or files merged
    return _read_geojson_url(f"{API_URL}/v1/geojson?{query}")


# --------------------------------------------------------------------------
# Use case 1: Velib stations by commune / arrondissement (use-case/usecase1.qmd)
# --------------------------------------------------------------------------


@pytest.mark.parametrize("year, path", LOCATIONS)
def test_usecase1_communes_arrondissements_petite_couronne(year, path):
    contours = download(
        year,
        path,
        values=PETITE_COURONNE,
        borders="COMMUNE_ARRONDISSEMENT",
        filter_by="DEPARTEMENT",
    )
    communes = download(
        year, path, values=PETITE_COURONNE, borders="COMMUNE", filter_by="DEPARTEMENT"
    )

    assert isinstance(contours, gpd.GeoDataFrame)
    assert contours.crs.to_epsg() == 4326
    assert set(contours["INSEE_DEP"].astype(str)) == set(PETITE_COURONNE)

    # Paris is replaced by its 20 arrondissements, INSEE_COG identifies them
    # and INSEE_COM is the code of Paris
    paris = contours[contours["INSEE_DEP"].astype(str) == "75"]
    assert len(paris) == 20
    assert set(paris["INSEE_COM"].astype(str)) == {"75056"}
    assert sorted(paris["INSEE_COG"].astype(str)) == [str(75101 + i) for i in range(20)]
    assert len(contours) == len(communes) - 1 + 20
    assert contours["INSEE_COG"].is_unique

    # What the use case does next: departements by dissolve, spatial join of
    # stations, count by INSEE_COG, areas in Lambert 93
    departements = contours.dissolve("INSEE_DEP")
    assert len(departements) == 4
    stations = gpd.GeoDataFrame(
        {"capacity": [30, 20]},
        geometry=[
            Point(2.3522, 48.8566),
            Point(2.2945, 48.8584),
        ],  # Hotel de Ville, Tour Eiffel
        crs=4326,
    )
    located = gpd.sjoin(stations, contours, predicate="within")
    assert len(located) == 2
    assert set(located["INSEE_COG"].astype(str)) == {"75104", "75107"}
    assert (contours.to_crs(2154).area > 0).all()


@pytest.mark.parametrize("year, path", LOCATIONS)
@pytest.mark.parametrize(
    "departement, city, arrondissements", [("69", "69123", 9), ("13", "13055", 16)]
)
def test_usecase1_lyon_marseille(year, path, departement, city, arrondissements):
    contours = download(
        year,
        path,
        values=departement,
        borders="COMMUNE_ARRONDISSEMENT",
        filter_by="DEPARTEMENT",
    )
    city_rows = contours[contours["INSEE_COM"].astype(str) == city]
    assert len(city_rows) == arrondissements
    assert city not in set(contours["INSEE_COG"].astype(str))


# --------------------------------------------------------------------------
# Filters on the attributes or the geometry (where, issue #112)
# --------------------------------------------------------------------------


@pytest.mark.parametrize("year, path", LOCATIONS)
def test_where_communes_plus_2000_occitanie(year, path):
    kwargs = {"values": "76", "borders": "COMMUNE", "filter_by": "REGION"}
    occitanie = download(year, path, **kwargs)
    plus_2000 = download(year, path, where="POPULATION > 2000", **kwargs)
    assert len(occitanie) > 4000
    assert 400 < len(plus_2000) < 700
    expected = occitanie.loc[occitanie["POPULATION"] > 2000, "INSEE_COM"]
    assert sorted(plus_2000["INSEE_COM"]) == sorted(expected)


@pytest.mark.parametrize("year, path", NEW_LOCATIONS)
def test_where_bounding_box_camargue(year, path):
    # Across two regions: Gard and Herault (Occitanie), Bouches-du-Rhone (PACA)
    communes = download(
        year,
        path,
        values="France",
        borders="COMMUNE",
        filter_by="FRANCE_ENTIERE",
        where="ST_Intersects(geometry, ST_MakeEnvelope(4.1, 43.3, 4.9, 43.75))",
    )
    assert set(communes["INSEE_DEP"]) == {"13", "30", "34"}
    assert "13004" in set(communes["INSEE_COM"])  # Arles


@pytest.mark.parametrize("year, path", NEW_LOCATIONS)
def test_where_distance_bugey(year, path):
    # Communes within 20 km of the Bugey nuclear plant (Saint-Vulbas, Ain)
    communes = download(
        year,
        path,
        values="France",
        borders="COMMUNE",
        filter_by="FRANCE_ENTIERE",
        where=(
            "ST_DWithin(ST_Transform(geometry, 'EPSG:4326', 'EPSG:2154'), "
            "ST_Transform(ST_Point(5.2706, 45.7983), 'EPSG:4326', 'EPSG:2154'), "
            "20000)"
        ),
    )
    assert set(communes["INSEE_DEP"]) == {"01", "38", "69"}
    assert "01390" in set(communes["INSEE_COM"])  # Saint-Vulbas
    distances = communes.to_crs(2154).distance(
        gpd.GeoSeries([Point(5.2706, 45.7983)], crs=4326).to_crs(2154).iloc[0]
    )
    assert (distances <= 20000).all()


@pytest.mark.parametrize("year, path", NEW_LOCATIONS)
def test_where_point_in_polygon(year, path):
    arrondissement = download(
        year,
        path,
        values="75",
        borders="COMMUNE_ARRONDISSEMENT",
        filter_by="DEPARTEMENT",
        where="ST_Contains(geometry, ST_Point(2.2945, 48.8584))",  # Tour Eiffel
    )
    assert list(arrondissement["INSEE_COG"]) == ["75107"]


# --------------------------------------------------------------------------
# Use case 2: livestock per inhabitant, DROM brought closer (use-case/usecase2.qmd)
# --------------------------------------------------------------------------


@pytest.mark.parametrize("year, path", LOCATIONS)
def test_usecase2_departements_drom_rapproches(year, path):
    with warnings.catch_warnings():
        # The use case asked for GeoJSON: GeoParquet is read from 2025 onwards
        warnings.simplefilter("ignore", UserWarning)
        departements = download(
            year,
            path,
            values="France",
            borders="DEPARTEMENT",
            vectorfile_format="geojson",
            filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
        )
    codes = departements["INSEE_DEP"].astype(str)
    assert codes.nunique() == 101
    if has_zoom_duplicates(year, path):
        # The 4 departements of the Ile-de-France zoom come out twice
        assert len(departements) == 105
        assert sorted(codes[codes.duplicated()]) == PETITE_COURONNE
    else:
        # One row per departement: the zoom is a part of its multipolygon
        assert len(departements) == 101
        paris = departements.loc[codes == "75"].geometry.iloc[0]
        assert paris.geom_type == "MultiPolygon" and len(paris.geoms) >= 2

    # Merge with departemental data, then a ratio on POPULATION
    cheptel = pd.DataFrame({"code": sorted(codes.unique()), "bovins": 1000})
    merged = departements.merge(cheptel, left_on="INSEE_DEP", right_on="code")
    assert len(merged) == len(departements)
    assert pd.api.types.is_numeric_dtype(merged["POPULATION"])
    assert (merged["POPULATION"] > 0).all()
    assert merged["bovins"].div(merged["POPULATION"]).notna().all()

    # The DROM are moved next to metropolitan France
    minx, miny, maxx, maxy = departements.total_bounds
    assert minx > -10 and maxx < 12 and miny > 40 and maxy < 53


@skip_with_api
@pytest.mark.parametrize("year, path", NEW_LOCATIONS)
def test_usecase2_geojson_forced_reads_geoparquet(year, path):
    # No GeoJSON file from 2025 onwards: force=True is ignored
    kwargs = {
        "values": "France",
        "borders": "DEPARTEMENT",
        "filter_by": "FRANCE_ENTIERE_DROM_RAPPROCHES",
    }
    parquet = download(year, path, **kwargs)
    with pytest.warns(UserWarning, match="force=True is ignored"):
        forced = download(year, path, vectorfile_format="geojson", force=True, **kwargs)
    assert sorted(parquet["INSEE_DEP"]) == sorted(forced["INSEE_DEP"])
    assert parquet["POPULATION"].sum() == forced["POPULATION"].sum()


# --------------------------------------------------------------------------
# Home page: France entiere maps (src/_programs.qmd, print_program_france)
# --------------------------------------------------------------------------

# Expected number of polygons, and of rows with the Ile-de-France zoom in the
# files of the former pipeline (see has_zoom_duplicates)
FRANCE_LEVELS = {
    "DEPARTEMENT": ("INSEE_DEP", 101, 105),
    "REGION": ("INSEE_REG", 18, 19),
    "BASSIN_VIE": (None, 1500, None),
    "AIRE_ATTRACTION_VILLES": (None, 650, None),
}


@pytest.mark.parametrize("year, path", LOCATIONS)
@pytest.mark.parametrize("level", list(FRANCE_LEVELS))
@pytest.mark.parametrize("drom_rapproches", [False, True])
@pytest.mark.parametrize("simplification", [0, 50])
def test_homepage_france(year, path, level, drom_rapproches, simplification):
    if (
        (year, path) == (LEGACY_YEAR, "production")
        and level == "AIRE_ATTRACTION_VILLES"
        and drom_rapproches
    ):
        pytest.xfail(
            "Files of the former pipeline: AAV2020 '000' (communes outside any "
            "attraction area) is one feature spanning all the territories, so the "
            "DROM are misplaced. Fixed since 0.2.0 (dissolve by territory)."
        )
    filter_by = (
        "FRANCE_ENTIERE_DROM_RAPPROCHES" if drom_rapproches else "FRANCE_ENTIERE"
    )
    gdf = download(
        year,
        path,
        values=["France"],
        borders=level,
        filter_by=filter_by,
        simplification=simplification,
    )
    code, n, n_drom = FRANCE_LEVELS[level]
    if code:
        assert gdf[code].nunique() == n
        zoom_rows = drom_rapproches and has_zoom_duplicates(year, path)
        assert len(gdf) == (n_drom if zoom_rows else n)
    else:
        assert len(gdf) > n
    assert gdf.crs.to_epsg() == 4326
    assert gdf.geometry.notna().all() and not gdf.geometry.is_empty.any()
    assert (gdf["PAYS"] == "France").all()

    minx, _, maxx, _ = gdf.total_bounds
    if drom_rapproches:
        assert minx > -10 and maxx < 12
    else:
        # True positions: from the Antilles (-61) to Mayotte / La Reunion (+55)
        assert minx < -60 and maxx > 55


@pytest.mark.parametrize("year, path", LOCATIONS)
@pytest.mark.parametrize("level", ["DEPARTEMENT", "REGION"])
def test_homepage_france_simplification_keeps_features(year, path, level):
    counts = {
        s: len(
            download(
                year,
                path,
                values="France",
                borders=level,
                filter_by="FRANCE_ENTIERE",
                simplification=s,
            )
        )
        for s in (0, 50)
    }
    assert counts[0] == counts[50]


# --------------------------------------------------------------------------
# Home page: communes of some departements (print_program_departement_single)
# --------------------------------------------------------------------------


@pytest.mark.parametrize("year, path", LOCATIONS)
@pytest.mark.parametrize("borders", ["COMMUNE", "COMMUNE_ARRONDISSEMENT"])
@pytest.mark.parametrize("departements", [["2A", "2B"], ["971"], ["01", "74"], ["13"]])
def test_homepage_communes_of_departements(year, path, borders, departements):
    gdf = download(
        year, path, values=departements, borders=borders, filter_by="DEPARTEMENT"
    )
    assert set(gdf["INSEE_DEP"].astype(str)) == set(departements)
    assert gdf[
        "INSEE_COG" if borders == "COMMUNE_ARRONDISSEMENT" else "INSEE_COM"
    ].is_unique
    assert (gdf["POPULATION"] >= 0).all()
    if set(departements) & DROM:
        assert gdf.total_bounds[0] < -60


@pytest.mark.parametrize("year, path", LOCATIONS)
def test_homepage_metadata(year, path):
    """Metadata table shown next to the maps."""
    gdf = download(year, path, values="13", borders="COMMUNE", filter_by="DEPARTEMENT")
    expected = {
        "ID",
        "NOM",
        "INSEE_COM",
        "STATUT",
        "POPULATION",
        "INSEE_DEP",
        "INSEE_REG",
        "LIBELLE_DEPARTEMENT",
        "LIBELLE_REGION",
        "EPCI",
        "ZE2020",
        "UU2020",
        "AAV2020",
        "PAYS",
        "SOURCE",
    }
    assert expected <= set(gdf.columns)
    marseille = gdf[gdf["INSEE_COM"].astype(str) == "13055"].iloc[0]
    assert marseille["NOM"] == "Marseille"
    assert marseille["LIBELLE_DEPARTEMENT"] == "Bouches-du-Rhône"
    assert marseille["LIBELLE_REGION"] == "Provence-Alpes-Côte d'Azur"


# --------------------------------------------------------------------------
# Same results with the DuckDB engine, and the formats no longer served
# --------------------------------------------------------------------------


@skip_with_api
@pytest.mark.parametrize("year, path", NEW_LOCATIONS)
def test_engine_duckdb_same_result(year, path):
    kwargs = {
        "values": PETITE_COURONNE,
        "borders": "COMMUNE_ARRONDISSEMENT",
        "filter_by": "DEPARTEMENT",
    }
    gdf = download(year, path, **kwargs)
    rel = download(year, path, engine="duckdb", **kwargs)
    assert isinstance(rel, duckdb.DuckDBPyRelation)
    assert rel.aggregate("count(*), sum(POPULATION)").fetchone() == (
        len(gdf),
        gdf["POPULATION"].sum(),
    )


@skip_with_api
def test_topojson_no_longer_supported():
    # Default format of the website examples: now refused explicitly
    with pytest.raises(ValueError, match="geojson' or 'parquet"):
        download(
            LEGACY_YEAR,
            "production",
            values="France",
            borders="DEPARTEMENT",
            filter_by="FRANCE_ENTIERE_DROM_RAPPROCHES",
            vectorfile_format="topojson",
        )
