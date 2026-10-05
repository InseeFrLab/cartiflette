"""
Time lost by reading GeoJSON through the API instead of the GeoJSON files.

The scenarios are the use cases of the integration tests of the client
(python-package/cartiflette/tests/test_integration.py). For each one:

- files: the GeoJSON files of the S3 storage, one per value, fetched in
  parallel (what the R and JS clients do);
- api: /v1/geojson, one request, gzip accepted;
- parquet: `carti_download` on the GeoParquet, the Python client path, as a
  reference (fresh DuckDB connection, so no cache).

Two measures: the GeoJSON text received (bytes, decompressed), and a
GeoDataFrame (text parsed with `geopandas.read_file`; parquet: the client).
The API is started by this script and restarted before each scenario: its
first request is "cold", the next ones "warm" (DuckDB caches the remote files
it has read: `enable_external_file_cache`).

    uv run python benchmark.py --repeat 5 --output benchmark.json
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import socket
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import duckdb
import geopandas as gpd
import pandas as pd
from cartiflette import carti_download
from cartiflette.constants import ENDPOINT_URL
from cartiflette.utils import create_path_bucket

PETITE_COURONNE = ["75", "92", "93", "94"]
# name: (borders, filter_by, values, simplification)
SCENARIOS = {
    "usecase1 arrondissements petite couronne": (
        "COMMUNE_ARRONDISSEMENT",
        "DEPARTEMENT",
        PETITE_COURONNE,
        50,
    ),
    "usecase1 arrondissements Lyon": (
        "COMMUNE_ARRONDISSEMENT",
        "DEPARTEMENT",
        ["69"],
        50,
    ),
    "usecase2 departements DROM rapproches": (
        "DEPARTEMENT",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
        ["France"],
        50,
    ),
    "home communes 13": ("COMMUNE", "DEPARTEMENT", ["13"], 50),
    "home communes 01 + 74": ("COMMUNE", "DEPARTEMENT", ["01", "74"], 50),
    "home communes 13 (simplification 0)": ("COMMUNE", "DEPARTEMENT", ["13"], 0),
    "home regions France": ("REGION", "FRANCE_ENTIERE", ["France"], 50),
    "home bassins de vie France (simplification 0)": (
        "BASSIN_VIE",
        "FRANCE_ENTIERE",
        ["France"],
        0,
    ),
    "home AAV DROM rapproches (simplification 0)": (
        "AIRE_ATTRACTION_VILLES",
        "FRANCE_ENTIERE_DROM_RAPPROCHES",
        ["France"],
        0,
    ),
}


def fetch(url: str) -> tuple[bytes, int]:
    """Content of a URL (decompressed) and number of bytes transferred."""
    request = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    with urllib.request.urlopen(request) as response:
        content = response.read()
        transferred = len(content)
        if response.headers.get("Content-Encoding") == "gzip":
            content = gzip.decompress(content)
    return content, transferred


def files_urls(year, path, borders, filter_by, values, simplification) -> list[str]:
    return [
        f"{ENDPOINT_URL}/"
        + create_path_bucket(
            provider="IGN",
            dataset_family="ADMINEXPRESS",
            source="EXPRESS-COG-CARTO-TERRITOIRE",
            year=year,
            borders=borders,
            crs=4326,
            filter_by=filter_by,
            value=value,
            vectorfile_format="geojson",
            territory="metropole",
            simplification=simplification,
            path_within_bucket=path,
        )
        for value in values
    ]


def api_url(api, year, path, borders, filter_by, values, simplification) -> str:
    query = urllib.parse.urlencode(
        {
            "year": year,
            "borders": borders,
            "filter_by": filter_by,
            "values": values,
            "simplification": simplification,
            "path_within_bucket": path,
        },
        doseq=True,
    )
    return f"{api}/v1/geojson?{query}"


def fetch_all(urls: list[str]) -> tuple[list[bytes], int]:
    with ThreadPoolExecutor(len(urls)) as pool:
        results = list(pool.map(fetch, urls))
    return [r[0] for r in results], sum(r[1] for r in results)


def to_geodataframe(contents: list[bytes]) -> gpd.GeoDataFrame:
    return pd.concat(
        [gpd.read_file(io.BytesIO(c)) for c in contents], ignore_index=True
    )


def timed(function) -> tuple[float, object]:
    start = time.perf_counter()
    result = function()
    return time.perf_counter() - start, result


def measure_geojson(urls: list[str]) -> dict:
    """Seconds for the text, then for the GeoDataFrame, with the sizes."""
    seconds, (contents, transferred) = timed(lambda: fetch_all(urls))
    parse_seconds, gdf = timed(lambda: to_geodataframe(contents))
    return {
        "text": seconds,
        "geodataframe": seconds + parse_seconds,
        "rows": len(gdf),
        "bytes": sum(len(c) for c in contents),
        "transferred": transferred,
    }


def measure_parquet(year, path, borders, filter_by, values, simplification) -> dict:
    seconds, gdf = timed(
        lambda: carti_download(
            values=values,
            borders=borders,
            filter_by=filter_by,
            year=year,
            simplification=simplification,
            path_within_bucket=path,
            con=duckdb.connect(),
        )
    )
    return {"geodataframe": seconds, "rows": len(gdf)}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_api() -> tuple[subprocess.Popen, str]:
    port = free_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "--factory",
            "cartiflette_api.app:create_app",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ]
    )
    api = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            fetch(f"{api}/health")
            return process, api
        except OSError:
            time.sleep(0.1)
    process.kill()
    raise RuntimeError("The API did not start")


def median(runs: list[dict], key: str) -> float:
    return statistics.median(r[key] for r in runs)


def run_scenario(year, path, scenario, repeat) -> dict:
    urls = files_urls(year, path, *scenario)
    process, api = start_api()
    try:
        url = api_url(api, year, path, *scenario)
        api_cold = measure_geojson([url])
        api_warm = [measure_geojson([url]) for _ in range(repeat)]
    finally:
        process.terminate()
        process.wait()
    files = [measure_geojson(urls) for _ in range(repeat)]
    parquet = [measure_parquet(year, path, *scenario) for _ in range(repeat)]
    rows = {api_cold["rows"], files[0]["rows"], parquet[0]["rows"]}
    if len(rows) != 1:
        raise AssertionError(f"Different numbers of rows: {rows}")
    return {
        "rows": rows.pop(),
        "files": len(urls),
        "bytes_files": files[0]["bytes"],
        "bytes_api": api_cold["bytes"],
        "transferred_files": files[0]["transferred"],
        "transferred_api": api_cold["transferred"],
        "text_files": median(files, "text"),
        "text_api_cold": api_cold["text"],
        "text_api_warm": median(api_warm, "text"),
        "gdf_files": median(files, "geodataframe"),
        "gdf_api_cold": api_cold["geodataframe"],
        "gdf_api_warm": median(api_warm, "geodataframe"),
        "gdf_parquet": median(parquet, "geodataframe"),
    }


# Bandwidths of a remote client, in Mbit/s, for the estimates
BANDWIDTHS = (20, 100)


def remote_seconds(local_seconds: float, transferred: int, mbps: float) -> float:
    """
    Time for a remote client: measured here (same network as the storage),
    plus the transfer at `mbps`. Conservative for the API, whose response is
    streamed: the transfer overlaps the query.
    """
    return local_seconds + transferred * 8 / (mbps * 1e6)


def markdown(results: dict) -> str:
    measured = [
        (
            "| Scénario | Lignes | Fichiers | Mo GeoJSON | Mo transférés API "
            "| Texte : fichiers | API froid | API chaud "
            "| GeoDataFrame : fichiers | API froid | API chaud | GeoParquet |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    keys = (
        "text_files",
        "text_api_cold",
        "text_api_warm",
        "gdf_files",
        "gdf_api_cold",
        "gdf_api_warm",
        "gdf_parquet",
    )
    remote = [
        "| Scénario | "
        + " | ".join(f"Fichiers {b} Mbit/s | API {b} Mbit/s" for b in BANDWIDTHS)
        + " |",
        "|---|" + "---:|---:|" * len(BANDWIDTHS),
    ]
    for name, r in results.items():
        measured.append(
            f"| {name} | {r['rows']} | {r['files']} | {r['bytes_files'] / 1e6:.2f} "
            f"| {r['transferred_api'] / 1e6:.2f} | "
            + " | ".join(f"{r[k]:.2f} s" for k in keys)
            + " |"
        )
        estimates = []
        for mbps in BANDWIDTHS:
            estimates += [
                remote_seconds(r["text_files"], r["transferred_files"], mbps),
                remote_seconds(r["text_api_cold"], r["transferred_api"], mbps),
            ]
        remote.append(
            f"| {name} | " + " | ".join(f"{t:.2f} s" for t in estimates) + " |"
        )
    return (
        "Mesuré (client dans le même réseau que le stockage, médianes) :\n\n"
        + "\n".join(measured)
        + "\n\nTexte GeoJSON pour un client distant (estimé, API à froid) :\n\n"
        + "\n".join(remote)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--year", type=int, default=2025)
    # GeoJSON files to compare with: the pipeline no longer writes them, the
    # last ones are in test/v0.2.0 (2025, 2026)
    parser.add_argument("--path-within-bucket", default="test/v0.2.0")
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--output", help="JSON file for the raw results")
    args = parser.parse_args()

    results = {}
    for name, scenario in SCENARIOS.items():
        print(f"{name}...", file=sys.stderr)
        results[name] = run_scenario(
            args.year, args.path_within_bucket, scenario, args.repeat
        )
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
    print(markdown(results))


if __name__ == "__main__":
    main()
