import json
import shutil
from unittest import mock

import pytest

from cartiflette import mapshaper, pipeline, prepare


def test_consolidate_and_upload_refuses_production_before_processing():
    with mock.patch.object(pipeline, "process_consolidated") as process:
        with pytest.raises(PermissionError):
            pipeline.consolidate_and_upload(
                2025,
                "inputs",
                "work",
                "REGION",
                "FRANCE_ENTIERE",
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
        # Paris is copied by the Ile-de-France zoom: still one row
        (
            "DEPARTEMENT",
            "FRANCE_ENTIERE_DROM_RAPPROCHES",
            3,
            {"FRANCE_ENTIERE_DROM_RAPPROCHES": "PAYS"},
        ),
        (
            "COMMUNE",
            "FRANCE_ENTIERE_DROM_RAPPROCHES",
            4,
            {"FRANCE_ENTIERE_DROM_RAPPROCHES": "PAYS"},
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
    test_filters = {
        **pipeline.FILTERS,
        "COMMUNE": ["BASSIN_VIE", "DEPARTEMENT", *pipeline.FILTERS["DEPARTEMENT"]],
    }

    with mock.patch.object(pipeline, "FILTERS", test_filters):
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
    if layout == "FRANCE_ENTIERE_DROM_RAPPROCHES":
        # The zoomed copy of Paris is a second part of the same feature
        assert con.execute(
            f"SELECT ST_NumGeometries(geometry) FROM '{parquet}' WHERE INSEE_DEP = '75'"
        ).fetchall() == [(2,)]
    if "REGION" in filters:
        # Sorted by region first
        regions = [
            r[0] for r in con.execute(f"SELECT INSEE_REG FROM '{parquet}'").fetchall()
        ]
        assert regions == sorted(regions)


IRIS = """(SELECT * FROM (VALUES
    ('010010101', '01001', '01001', '01', '84', 'metropole', 'France', '01004', ST_GeomFromText('POLYGON((5 46, 5.05 46, 5.05 46.1, 5 46.1, 5 46))')),
    ('010010102', '01001', '01001', '01', '84', 'metropole', 'France', '01004', ST_GeomFromText('POLYGON((5.05 46, 5.1 46, 5.1 46.1, 5.05 46.1, 5.05 46))')),
    ('751010101', '75056', '75101', '75', '11', 'metropole', 'France', '75056', ST_GeomFromText('POLYGON((2.3 48.8, 2.4 48.8, 2.4 48.9, 2.3 48.9, 2.3 48.8))')),
    ('971050000', '97105', '97105', '971', '01', 'guadeloupe', 'France', '97105', ST_GeomFromText('POLYGON((-61.6 16.0, -61.5 16.0, -61.5 16.1, -61.6 16.1, -61.6 16.0))'))
) t(CODE_IRIS, INSEE_COM, INSEE_COG, INSEE_DEP, INSEE_REG, AREA, PAYS, BV2022, geometry))"""


@needs_mapshaper
@pytest.mark.parametrize(
    "layout, filters",
    [
        (
            "FRANCE_ENTIERE",
            {
                "COMMUNE": "INSEE_COM",
                "COMMUNE_ARRONDISSEMENT": "INSEE_COG",
                "BASSIN_VIE": "BV2022",
                "DEPARTEMENT": "INSEE_DEP",
                "REGION": "INSEE_REG",
                "TERRITOIRE": "AREA",
                "FRANCE_ENTIERE": "PAYS",
            },
        ),
        ("FRANCE_ENTIERE_DROM_RAPPROCHES", {"FRANCE_ENTIERE_DROM_RAPPROCHES": "PAYS"}),
    ],
)
def test_process_consolidated_iris(tmp_path, layout, filters):
    """IRIS are read as is (no dissolve), filtered by commune."""
    con = prepare.connect()
    prepare.write_geojson(con, f"SELECT * FROM {IRIS}", str(tmp_path / "IRIS.geojson"))
    (tmp_path / "fields.json").write_text(json.dumps({"BASSIN_VIE": "BV2022"}))
    test_filters = {
        **pipeline.FILTERS,
        "IRIS": [
            "COMMUNE",
            "COMMUNE_ARRONDISSEMENT",
            "BASSIN_VIE",
            "DEPARTEMENT",
            *pipeline.FILTERS["DEPARTEMENT"],
        ],
    }

    with mock.patch.object(pipeline, "FILTERS", test_filters):
        parquet = pipeline.process_consolidated(
            str(tmp_path), str(tmp_path / "work"), "IRIS", layout, 0, 4326
        )

    kv = dict(
        con.execute(
            f"SELECT decode(key), decode(value) FROM parquet_kv_metadata('{parquet}')"
        ).fetchall()
    )
    assert json.loads(kv["cartiflette:filter_columns"]) == filters
    rows = con.execute(
        f"SELECT CODE_IRIS, ST_NumGeometries(geometry) FROM '{parquet}'"
    ).fetchall()
    # Sorted by departement and IRIS; Paris is zoomed in the DROM-rapproches
    # layout: a second part of the same feature
    paris_parts = 2 if layout == "FRANCE_ENTIERE_DROM_RAPPROCHES" else 1
    assert rows == [
        ("010010101", 1),
        ("010010102", 1),
        ("751010101", paris_parts),
        ("971050000", 1),
    ]


def test_available_levels(tmp_path):
    for level in ("COMMUNE", "COMMUNE_ARRONDISSEMENT"):
        (tmp_path / f"{level}.geojson").touch()
    # No IRIS before 2025
    assert "IRIS" not in pipeline.available_levels(str(tmp_path))
    assert "REGION" in pipeline.available_levels(str(tmp_path))
    (tmp_path / "IRIS.geojson").touch()
    assert pipeline.available_levels(str(tmp_path)) == list(pipeline.FILTERS)


def test_consolidated_combinations():
    jobs = pipeline.consolidated_combinations()
    # 9 levels x 2 geometries x 3 simplifications x 1 crs
    assert len(jobs) == 54
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
        # Commune of the rows contiguous: no region first
        ("IRIS", "FRANCE_ENTIERE", ["DEPARTEMENT", "IRIS"]),
        ("IRIS", "FRANCE_ENTIERE_DROM_RAPPROCHES", ["IRIS"]),
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
    parquet = pipeline.process_consolidated(
        str(tmp_path), str(tmp_path / "work"), "BASSIN_VIE", "FRANCE_ENTIERE", 0, 4326
    )
    # Living area 01004 spans metropole and Guadeloupe: one row per territory
    rows = con.execute(
        f"SELECT BV2022, AREA, POPULATION FROM '{parquet}' "
        "WHERE BV2022 = '01004' ORDER BY AREA"
    ).fetchall()
    assert rows == [("01004", "guadeloupe", 5), ("01004", "metropole", 30)]


@pytest.mark.parametrize(
    "simplification, expected",
    [
        (0, []),
        (50, ["-simplify", "50%", "keep-shapes"]),
        (80, ["-simplify", "20%", "keep-shapes"]),
    ],
)
def test_simplification_is_the_share_of_points_removed(simplification, expected):
    # mapshaper keeps the given share of points: 80 % removed = 20 % kept
    commands = mapshaper._finalize_commands(4326, simplification, "IGN")
    simplify = (
        commands[commands.index("-simplify") : commands.index("-simplify") + 3]
        if "-simplify" in commands
        else []
    )
    assert simplify == expected
