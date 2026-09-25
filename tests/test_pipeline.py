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
    ('75056', '75', '11', 'Paris', 'IDF', 'metropole', 30, 'France', '75056', ST_GeomFromText('POLYGON((2.3 48.8, 2.4 48.8, 2.4 48.9, 2.3 48.9, 2.3 48.8))'))
) t(INSEE_COM, INSEE_DEP, INSEE_REG, LIBELLE_DEPARTEMENT, LIBELLE_REGION, AREA, POPULATION, PAYS, BV2022, geometry))"""


@needs_mapshaper
@pytest.mark.parametrize(
    "level, filter_by, expected_files",
    [
        ("COMMUNE", "DEPARTEMENT", ["01.geojson", "75.geojson"]),
        ("DEPARTEMENT", "REGION", ["11.geojson", "84.geojson"]),
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

    parquet = pipeline.geojson_to_parquet(files[0], str(tmp_path / "out.parquet"))
    columns = [c[0] for c in con.execute(f"DESCRIBE '{parquet}'").fetchall()]
    assert "geometry" in columns and "SOURCE" in columns
