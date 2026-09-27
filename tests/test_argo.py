"""Safety checks on the Argo workflow (argo-pipeline/pipeline.yaml)."""

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from cartiflette import __version__
from cartiflette.pipeline import combinations, consolidated_combinations

ROOT = Path(__file__).parents[1]
WORKFLOW = yaml.safe_load((ROOT / "argo-pipeline" / "pipeline.yaml").read_text())
PARAMETERS = {
    p["name"]: p["value"] for p in WORKFLOW["spec"]["arguments"]["parameters"]
}
TEMPLATES = {t["name"]: t for t in WORKFLOW["spec"]["templates"]}


def test_defaults_write_to_test_location():
    assert PARAMETERS["path"] == f"test/v{__version__}"
    assert PARAMETERS["allow_production_write"] == "false"


def test_image_matches_pipeline_version():
    pyproject = (ROOT / "pyproject.toml").read_text()
    assert re.search(r'^version = "(.+)"', pyproject, re.MULTILINE)[1] == __version__
    assert PARAMETERS["image"] == f"inseefrlab/cartiflette:v{__version__}"


def test_resources_are_limited_and_cleaned():
    spec = WORKFLOW["spec"]
    assert spec["parallelism"] <= 10
    assert TEMPLATES["main"]["parallelism"] <= 2
    assert spec["activeDeadlineSeconds"] > 0
    assert spec["podGC"]["strategy"] == "OnPodSuccess"
    assert spec["ttlStrategy"]["secondsAfterSuccess"] > 0
    assert spec["ttlStrategy"]["secondsAfterFailure"] > 0


def test_logs_are_archived_outside_published_files():
    location = WORKFLOW["spec"]["templateDefaults"]["archiveLocation"]
    assert location["archiveLogs"] is True
    assert location["s3"]["bucket"] == "projet-cartiflette"
    assert location["s3"]["key"].startswith("argo-logs/")


def test_final_status_is_logged():
    # Read by argo-pipeline/logs-app: the exit handler's log is the only trace
    # of the status once the workflow is deleted
    assert WORKFLOW["spec"]["onExit"] == "report-status"
    container = TEMPLATES["report-status"]["container"]
    assert container["command"] == ["echo"]
    (line,) = container["args"]
    for variable in ("status", "failures"):
        assert f"{{{{workflow.{variable}}}}}" in line


def test_years_is_a_json_list():
    assert all(y.isdigit() for y in json.loads(PARAMETERS["years"]))


def test_target_checked_before_anything_else():
    tasks = {t["name"]: t for t in TEMPLATES["main"]["dag"]["tasks"]}
    assert "dependencies" not in tasks["check-target"]
    assert tasks["year"]["dependencies"] == ["check-target"]
    # Every step gets the write target and the production switch
    for name in ("check-target", "prepare", "list-jobs", "split", "consolidate"):
        env = {e["name"]: e.get("value") for e in TEMPLATES[name]["container"]["env"]}
        assert env["CARTIFLETTE_WRITE_PATH"] == "{{workflow.parameters.path}}"
        assert (
            env["CARTIFLETTE_ALLOW_PRODUCTION_WRITE"]
            == "{{workflow.parameters.allow_production_write}}"
        )


@pytest.mark.parametrize(
    "path, allow, ok",
    [
        ("test/v0.2.0", "false", True),
        ("production", "false", False),
        ("production/", "yes", False),
        ("production", "true", True),
        ("production", "True", True),
    ],
)
def test_check_target_script(path, allow, ok):
    result = subprocess.run(
        [sys.executable, str(ROOT / "argo-pipeline" / "src" / "check_target.py")],
        env={
            "CARTIFLETTE_WRITE_PATH": path,
            "CARTIFLETTE_ALLOW_PRODUCTION_WRITE": allow,
            "PYTHONPATH": str(ROOT),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is ok, result.stderr
    if not ok:
        assert "PermissionError" in result.stderr


@pytest.mark.parametrize(
    "kind, jobs",
    [("geojson", combinations()), ("parquet", consolidated_combinations())],
)
def test_crossproduct_matches_workflow_items(kind, jobs):
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "argo-pipeline" / "src" / "crossproduct.py"),
            "--kind",
            kind,
        ],
        env={"PYTHONPATH": str(ROOT)},
        capture_output=True,
        text=True,
        check=True,
    )
    items = json.loads(result.stdout)
    assert len(items) == len(jobs)
    # Keys used as {{item.xxx}} in the split / consolidate tasks
    task = "split" if kind == "geojson" else "consolidate"
    tasks = {t["name"]: t for t in TEMPLATES["year"]["dag"]["tasks"]}
    used = {
        a["value"][len("{{item.") : -2]
        for a in tasks[task]["arguments"]["parameters"]
        if a["value"].startswith("{{item.")
    }
    assert used == set(items[0])
