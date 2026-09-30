"""
Integration tests: the classic uses shown on the cartiflette website
(https://github.com/InseeFrLab/cartiflette-website, use-case/ and the
interactive home page), run against published files, read-only.

Deselected by default, run them with:

    uv run pytest -m integration

The new vintages are read from CARTIFLETTE_TEST_PATH (default "test/v0.2.0",
the test location of the pipeline) for the years in CARTIFLETTE_TEST_YEARS
(default "2025,2026"; add 2022, 2023, 2024 once produced from the IGN
editions 3-x). The 2022 files are read from production, to check that
the files published by the former pipeline are still readable.

With CARTIFLETTE_API_URL (e.g. "http://localhost:8000", see api/), the same
use cases read GeoJSON from the API instead of the files: /v1/geojson when
there is GeoParquet, the legacy file path (redirected to the file) otherwise.
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
from cartiflette.utils import create_path_bucket
from shapely.geometry import Point

from cartiflette import carti_download

pytestmark = pytest.mark.integration

TEST_PATH = os.environ.get("CARTIFLETTE_TEST_PATH", "test/v0.2.0")
YEARS = [
    int(y) for y in os.environ.get("CARTIFLETTE_TEST_YEARS", "2025,2026").split(",")
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
    year, path, values, borders, filter_by, crs, simplification, **_format
):
    """Same polygons as `carti_download`, as GeoJSON from the API."""
    values = [values] if isinstance(values, (str, int)) else values
    if year != LEGACY_YEAR:
        query = urllib.parse.urlencode(
            {
                "year": year,
                "borders": borders,
                "filter_by": filter_by,
                "values": values,
                "crs": crs,
                "simplification": simplification,
                "path_within_bucket": path,
            },
            doseq=True,
        )
        return _read_geojson_url(f"{API_URL}/v1/geojson?{query}")
    # No GeoParquet: legacy paths, which the API redirects to the files
    return pd.concat(
        [
            _read_geojson_url(
                f"{API_URL}/"
                + create_path_bucket(
                    provider="IGN",
                    dataset_family="ADMINEXPRESS",
                    source="EXPRESS-COG-CARTO-TERRITOIRE",
                    year=year,
                    borders=borders,
                    crs=crs,
                    filter_by=filter_by,
                    value=value,
                    vectorfile_format="geojson",
                    territory="metropole",
                    simplification=simplification,
                    path_within_bucket=path,
                )
            )
            for value in values
        ],
        ignore_index=True,
    )


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
    # 101 departements, and the 4 departements of the Ile-de-France zoom
    assert codes.nunique() == 101
    assert len(departements) == 105
    assert sorted(codes[codes.duplicated()]) == PETITE_COURONNE

    # Merge with departemental data, then a ratio on POPULATION
    cheptel = pd.DataFrame({"code": sorted(codes.unique()), "bovins": 1000})
    merged = departements.merge(cheptel, left_on="INSEE_DEP", right_on="code")
    assert len(merged) == 105
    assert pd.api.types.is_numeric_dtype(merged["POPULATION"])
    assert (merged["POPULATION"] > 0).all()
    assert merged["bovins"].div(merged["POPULATION"]).notna().all()

    # The DROM are moved next to metropolitan France
    minx, miny, maxx, maxy = departements.total_bounds
    assert minx > -10 and maxx < 12 and miny > 40 and maxy < 53


@skip_with_api
@pytest.mark.parametrize("year, path", NEW_LOCATIONS)
def test_usecase2_geojson_forced_equals_geoparquet(year, path):
    kwargs = {
        "values": "France",
        "borders": "DEPARTEMENT",
        "filter_by": "FRANCE_ENTIERE_DROM_RAPPROCHES",
    }
    parquet = download(year, path, **kwargs)
    with pytest.warns(UserWarning, match="force=True"):
        geojson = download(
            year, path, vectorfile_format="geojson", force=True, **kwargs
        )
    assert sorted(parquet["INSEE_DEP"]) == sorted(geojson["INSEE_DEP"])
    assert parquet["POPULATION"].sum() == geojson["POPULATION"].sum()


# --------------------------------------------------------------------------
# Home page: France entiere maps (src/_programs.qmd, print_program_france)
# --------------------------------------------------------------------------

# Expected number of polygons, without / with the Ile-de-France zoom
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
        assert len(gdf) == (n_drom if drom_rapproches else n)
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
