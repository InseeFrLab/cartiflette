import json
import shutil
from unittest import mock

import pytest

from cartiflette import pipeline, prepare


def test_combinations():
    jobs = pipeline.combinations()
    # 37 (level, filter_by) pairs x 2 simplifications x 1 crs
    assert len(jobs) == 74
    assert {
        "level_polygons": "COMMUNE_ARRONDISSEMENT",
        "filter_by": "DEPARTEMENT",
        "simplification": 50,
        "crs": 4326,
    } in jobs
    assert {j["level_polygons"] for j in pipeline.combinations(["REGION"])} == {
        "REGION"
    }


def test_split_and_upload_refuses_production_before_processing():
    with mock.patch.object(pipeline, "process") as process:
        with pytest.raises(PermissionError):
            pipeline.split_and_upload(
                2025,
                "inputs",
                "work",
                "REGION",
                "TERRITOIRE",
                0,
                4326,
                fs=mock.Mock(),
                bucket="projet-cartiflette",
                path_within_bucket="production",
            )
        process.assert_not_called()


needs_mapshaper = pytest.mark.skipif(
    shutil.which("mapshaper") is None, reason="mapshaper is not installed"
)

COMMUNES = """(SELECT * FROM (VALUES
    ('01001', '01', '84', 'Ain', 'ARA', 'metropole', 10, 'France', '01004', ST_GeomFromText('POLYGON((5 46, 5.1 46, 5.1 46.1, 5 46.1, 5 46))')),
    ('01002', '01', '84', 'Ain', 'ARA', 'metropole', 20, 'France', '01004', ST_GeomFromText('POLYGON((5.1 46, 5.2 46, 5.2 46.1, 5.1 46.1, 5.1 46))')),
    ('75056', '75', '11', 'Paris', 'IDF', 'metropole', 30, 'France', '75056', ST_GeomFromText('POLYGON((2.3 48.8, 2.4 48.8, 2.4 48.9, 2.3 48.9, 2.3 48.8))')),
    -- Same living area code as 01001 and 01002, in another territory: must
    -- not be merged with them
    ('97105', '971', '01', 'Guadeloupe', 'Guadeloupe', 'guadeloupe', 5, 'France', '01004', ST_GeomFromText('POLYGON((-61.6 16.0, -61.5 16.0, -61.5 16.1, -61.6 16.1, -61.6 16.0))'))
) t(INSEE_COM, INSEE_DEP, INSEE_REG, LIBELLE_DEPARTEMENT, LIBELLE_REGION, AREA, POPULATION, PAYS, BV2022, geometry))"""


@needs_mapshaper
@pytest.mark.parametrize(
    "level, filter_by, expected_files",
    [
        ("COMMUNE", "DEPARTEMENT", ["01.geojson", "75.geojson", "971.geojson"]),
        ("DEPARTEMENT", "REGION", ["01.geojson", "11.geojson", "84.geojson"]),
        ("BASSIN_VIE", "FRANCE_ENTIERE", ["France.geojson"]),
    ],
)
def test_process(tmp_path, level, filter_by, expected_files):
    con = prepare.connect()
    prepare.write_geojson(
        con, f"SELECT * FROM {COMMUNES}", str(tmp_path / "COMMUNE.geojson")
    )
    (tmp_path / "fields.json").write_text(json.dumps({"BASSIN_VIE": "BV2022"}))

    files = pipeline.process(
        str(tmp_path), str(tmp_path / "work"), level, filter_by, 0, 4326
    )
    assert [f.rsplit("/", 1)[-1] for f in files] == expected_files


@needs_mapshaper
@pytest.mark.parametrize(
    "level, layout, rows, filters",
    [
        (
            "COMMUNE",
            "FRANCE_ENTIERE",
            4,
            {
                "BASSIN_VIE": "BV2022",
                "DEPARTEMENT": "INSEE_DEP",
                "REGION": "INSEE_REG",
                "TERRITOIRE": "AREA",
                "FRANCE_ENTIERE": "PAYS",
            },
        ),
        (
            "DEPARTEMENT",
            "FRANCE_ENTIERE",
            3,
            {"REGION": "INSEE_REG", "TERRITOIRE": "AREA", "FRANCE_ENTIERE": "PAYS"},
        ),
    ],
)
def test_process_consolidated(tmp_path, level, layout, rows, filters):
    con = prepare.connect()
    prepare.write_geojson(
        con, f"SELECT * FROM {COMMUNES}", str(tmp_path / "COMMUNE.geojson")
    )
    (tmp_path / "fields.json").write_text(json.dumps({"BASSIN_VIE": "BV2022"}))
    # Only the zoning present in the tiny test data
    splits = {
        **pipeline.SPLITS,
        "COMMUNE": ["BASSIN_VIE", "DEPARTEMENT", *pipeline.SPLITS["DEPARTEMENT"]],
    }

    with mock.patch.object(pipeline, "SPLITS", splits):
        parquet = pipeline.process_consolidated(
            str(tmp_path), str(tmp_path / "work"), level, layout, 0, 4326
        )

    kv = dict(
        con.execute(
            f"SELECT decode(key), decode(value) FROM parquet_kv_metadata('{parquet}')"
        ).fetchall()
    )
    assert json.loads(kv["cartiflette:filter_columns"]) == filters
    geo = json.loads(kv["geo"])
    assert geo["version"] == "1.1.0"
    assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]
    columns = [c[0] for c in con.execute(f"DESCRIBE '{parquet}'").fetchall()]
    assert {"geometry", "bbox", "SOURCE", *filters.values()} <= set(columns)
    assert con.execute(f"SELECT count(*) FROM '{parquet}'").fetchone()[0] == rows
    # Sorted by region first
    regions = [
        r[0] for r in con.execute(f"SELECT INSEE_REG FROM '{parquet}'").fetchall()
    ]
    assert regions == sorted(regions)


def test_consolidated_combinations():
    jobs = pipeline.consolidated_combinations()
    # 8 levels x 2 geometries x 2 simplifications x 1 crs
    assert len(jobs) == 32
    assert {
        "level_polygons": "COMMUNE",
        "layout": "FRANCE_ENTIERE_DROM_RAPPROCHES",
        "simplification": 0,
        "crs": 4326,
    } in jobs


def test_filter_levels():
    assert pipeline.filter_levels("REGION", "FRANCE_ENTIERE") == [
        "TERRITOIRE",
        "FRANCE_ENTIERE",
    ]
    assert pipeline.filter_levels("REGION", "FRANCE_ENTIERE_DROM_RAPPROCHES") == [
        "FRANCE_ENTIERE_DROM_RAPPROCHES"
    ]


@pytest.mark.parametrize(
    "level, layout, expected",
    [
        (
            "COMMUNE",
            "FRANCE_ENTIERE",
            ["REGION", "DEPARTEMENT", "BASSIN_VIE", "COMMUNE"],
        ),
        (
            "COMMUNE_ARRONDISSEMENT",
            "FRANCE_ENTIERE",
            ["REGION", "DEPARTEMENT", "BASSIN_VIE", "COMMUNE_ARRONDISSEMENT"],
        ),
        ("DEPARTEMENT", "FRANCE_ENTIERE", ["REGION", "DEPARTEMENT"]),
        ("REGION", "FRANCE_ENTIERE", ["REGION"]),
        ("BASSIN_VIE", "FRANCE_ENTIERE", ["TERRITOIRE", "BASSIN_VIE"]),
        ("DEPARTEMENT", "FRANCE_ENTIERE_DROM_RAPPROCHES", ["DEPARTEMENT"]),
        ("ZONE_EMPLOI", "FRANCE_ENTIERE_DROM_RAPPROCHES", ["ZONE_EMPLOI"]),
    ],
)
def test_sort_levels(level, layout, expected):
    levels = pipeline.filter_levels(level, layout)
    assert pipeline.sort_levels(level, levels) == expected


@needs_mapshaper
def test_dissolve_keeps_territories_apart(tmp_path):
    con = prepare.connect()
    prepare.write_geojson(
        con, f"SELECT * FROM {COMMUNES}", str(tmp_path / "COMMUNE.geojson")
    )
    (tmp_path / "fields.json").write_text(json.dumps({"BASSIN_VIE": "BV2022"}))
    files = pipeline.process(
        str(tmp_path), str(tmp_path / "work"), "BASSIN_VIE", "TERRITOIRE", 0, 4326
    )
    assert [f.rsplit("/", 1)[-1] for f in files] == [
        "guadeloupe.geojson",
        "metropole.geojson",
    ]
    rows = con.execute(
        f"SELECT BV2022, AREA, POPULATION FROM ST_Read('{files[0]}')"
    ).fetchall()
    assert rows == [("01004", "guadeloupe", 5)]
