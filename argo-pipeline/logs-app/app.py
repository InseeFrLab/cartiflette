"""
Browse the Argo logs archived on S3 (projet-cartiflette/argo-logs/).

Logs are stored as <workflow>/<pod>/main.log. The final status comes from the
log of the exit handler (template `report-status` of pipeline.yaml): one JSON
line with the workflow status and its failed nodes. Workflows without it are
still running, or ran before the exit handler was added.

Read-only. Run with the AWS_* credentials of the environment:

    uv run --group logs streamlit run argo-pipeline/logs-app/app.py
"""

import json

import pandas as pd
import s3fs
import streamlit as st

ENDPOINT_URL = "https://minio.lab.sspcloud.fr"
LOGS_PREFIX = "projet-cartiflette/argo-logs/"
# Height of the log viewer in pixels, scrollable beyond.
LOG_HEIGHT = 600
# Pod names are <workflow>-<template>-<hash> (Argo POD_NAMES v2)
REPORT_TEMPLATE = "-report-status-"


@st.cache_resource
def get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URL})


def read_log(path: str) -> str:
    return get_fs().cat_file(path).decode("utf-8", errors="replace")


def parse_report(text: str) -> dict | None:
    """Status written by the exit handler, None if unreadable."""
    lines = text.strip().splitlines()
    try:
        report = json.loads(lines[-1])
    except (IndexError, json.JSONDecodeError):
        return None
    report["failures"] = report.get("failures") or []
    return report


def step_status(workflow: str, pod: str, reports: dict) -> str:
    report = reports.get(workflow)
    if report is None:
        return "inconnu"
    if REPORT_TEMPLATE in pod:
        return report["status"]
    failed = {f.get("podName") for f in report["failures"]}
    return "échec" if pod in failed else "ok"


@st.cache_data(ttl=60)
def list_logs(prefix: str) -> tuple[pd.DataFrame, dict]:
    """
    One row per log file (path relative to the prefix, status, date, size),
    and the exit handler report of each workflow that has one.
    """
    files = {
        path.removeprefix(prefix): info
        for path, info in get_fs().find(prefix, detail=True).items()
        if info["type"] == "file"
    }
    reports = {}
    for name in files:
        workflow, pod = name.split("/")[:2]
        if REPORT_TEMPLATE in pod:
            report = parse_report(read_log(prefix + name))
            if report is not None:
                reports[workflow] = report
    rows = []
    for name, info in files.items():
        workflow, pod = name.split("/")[:2]
        report = reports.get(workflow)
        rows.append(
            {
                "log": name,
                "étape": step_status(workflow, pod, reports),
                "workflow": report["status"] if report else "inconnu",
                "modifié": info.get("LastModified"),
                "taille (ko)": round(info["size"] / 1024, 1),
            }
        )
    df = pd.DataFrame(
        rows, columns=["log", "étape", "workflow", "modifié", "taille (ko)"]
    )
    return df.sort_values("modifié", ascending=False, ignore_index=True), reports


def main() -> None:
    st.set_page_config(page_title="Logs Argo cartiflette", layout="wide")
    st.title("Logs Argo")

    if st.button("Rafraîchir la liste"):
        list_logs.clear()

    logs, reports = list_logs(LOGS_PREFIX)
    if logs.empty:
        st.info(f"Aucun log sous s3://{LOGS_PREFIX}")
        return

    query = st.text_input("Rechercher un log par nom")
    if query:
        logs = logs[logs["log"].str.contains(query, case=False, regex=False)]
    if st.checkbox("Seulement les étapes en échec"):
        logs = logs[logs["étape"] == "échec"]
    logs = logs.reset_index(drop=True)
    st.caption(f"{len(logs)} log(s)")

    event = st.dataframe(
        logs,
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
    )
    selected = event.selection.rows
    if not selected:
        st.caption("Cliquer sur une ligne pour afficher le log.")
        return

    name = logs.loc[selected[0], "log"]
    st.subheader(name)
    workflow, pod = name.split("/")[:2]
    # Argo message of the failure: the only explanation when the pod was
    # killed (OOMKilled, deadline...) without writing anything to its log
    for failure in reports.get(workflow, {}).get("failures", []):
        if failure.get("podName") == pod:
            st.error(f"{failure.get('phase')} : {failure.get('message')}")
    st.code(
        read_log(LOGS_PREFIX + name),
        language="log",
        line_numbers=True,
        height=LOG_HEIGHT,
    )


main()
