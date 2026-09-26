# cartiflette

Two separate things live in this repo:

- `cartiflette/` + `argo-pipeline/`: the **production pipeline** (dist name `cartiflette-pipeline`, not on PyPI). It fetches IGN and Insee sources, processes them with mapshaper and DuckDB, and writes GeoJSON and GeoParquet files to S3 (MinIO, SSPCloud).
- `python-package/cartiflette/`: the **client** published on PyPI as `cartiflette`. It reads those files over HTTPS with DuckDB (GeoJSON: `read_json` over the list of per-value links; GeoParquet: one link filtered in SQL), and converts to geopandas only at the end (`engine="geopandas"`, default) or returns the DuckDB relation (`engine="duckdb"`). GeoParquet is read whenever it exists (from 2025 onwards), even when GeoJSON is requested, with a warning; GeoJSON is only read before 2025 or with `force=True` (also with a warning). It has its own `pyproject.toml`, `uv.lock` and tests; Python >= 3.10, DuckDB >= 1.5.

The only contract between the two is the S3 layout. Paths are built by `cartiflette/paths.py` and by `python-package/cartiflette/cartiflette/utils.py` (`create_path_bucket` for GeoJSON, `create_path_consolidated` for GeoParquet), which must stay identical. Both test suites pin the same expected strings. The `cartiflette:filter_columns` metadata of the GeoParquet files is part of the contract too.

## S3 safety: never write to production

- `projet-cartiflette/production` is read live by the Python, R and JS clients. Never write there.
- All writes go through `cartiflette/s3.py::upload`. It raises unless `CARTIFLETTE_ALLOW_PRODUCTION_WRITE=i-know-what-i-am-doing` is set. Don't bypass it: no direct `fs.put*` and no `mc` commands.
- The default write target is `projet-cartiflette/test/v<version>` (a fresh prefix per version: `test/` already holds files from earlier tests, never write at its root), overridable with `CARTIFLETTE_WRITE_BUCKET` and `CARTIFLETTE_WRITE_PATH`.
- Reading published files over public HTTPS is fine.
- Ask before running anything that writes to S3, even to the test location.

## Code style

- Functional code: plain functions and dicts, no classes. The previous object-oriented version (`Dataset`, `Layer`, `MasterScraper`) was removed on purpose; don't reintroduce it.
- Pass paths, `fs` and targets explicitly. No module-level side effects: no S3 client created at import, no `print`, no hard-coded `temp/` relative to the cwd.
- Call mapshaper with `subprocess.run([...])` using an argument list, never `shell=True`.
- Follow conventions based on Ruff styleguide. Use `ruff` and `vulture` to check for codebase after tasks. 

## Pipeline

`prepare_year` → `combinations` → `split_and_upload` (see `cartiflette/pipeline.py`). The Argo steps are in `argo-pipeline/src/`.

- **IGN source.** ADMIN EXPRESS COG CARTO editions 4-0 (2025+), "France entière" WGS84. The catalogue is an Atom feed at `https://data.geopf.fr/chunk/telechargement/resource/ADMIN-EXPRESS-COG-CARTO`. GeoParquet exists from 2026 only, 2025 is GPKG only. Editions ≤2024 (3-x, shapefile per territory) are not supported, and the already published years are not regenerated.
- **Rate limit.** The Géoplateforme allows 1 request per second, so `cartiflette/http.py` retries on 429.
- **Field names.** v4 renamed every field (`code_insee`, `population`…). `prepare.py` maps them back to the historical names (`ID`, `NOM`, `INSEE_COM`, `STATUT`, `POPULATION`, `AREA`) so that the published attributes stay stable.
- **Insee TAGC.** `table-appartenance-geo-communes-{year}.zip`, with the header on row 6. Zoning field vintages change over time (`BV2012` became `BV2022`), so they are resolved at runtime into `fields.json`, never hard-coded.
- **Territories.** Metropole and the 5 DROM. Saint-Pierre-et-Miquelon (`code_insee_du_departement = 'NR'`) is excluded. `mapshaper.dissolve` groups by code **and** territory (`AREA`): some codes span territories (AAV2020 `000`, communes outside any attraction area), and one feature over metropole + DROM breaks `bring_drom_closer`.
- **Formats.** GeoJSON and GeoParquet only, with two different layouts:
  - GeoJSON: one file per value of the split level (`{FILTER_BY}={value}` in the path), as before.
  - GeoParquet: one consolidated file per (year, level, crs, simplification, `layout`), where `layout` is `FRANCE_ENTIERE` or `FRANCE_ENTIERE_DROM_RAPPROCHES` (DROM moved, IDF zoom: different geometries, so a separate file). Never name a path segment after a column (the segment was `geometry=` at first): DuckDB and pyarrow read `key=value` segments as hive partitions, which shadowed the geometry column. Rows are sorted by `pipeline.sort_levels` (communes: region, departement, living area, commune; departements: region, departement; zonings: territory, code) in row groups of 2048 rows (DuckDB minimum), with a `bbox` column declared as GeoParquet 1.1 covering. The metadata key `cartiflette:filter_columns` maps each usable `filter_by` to its column (e.g. `BASSIN_VIE` -> `BV2022`). The client filters it with a SQL `WHERE` in DuckDB (`read_parquet(..., hive_partitioning = false)`), which only downloads the needed row groups thanks to the sort, the statistics and the bloom filters DuckDB writes (measured: ~2 MB for a departement out of 35 MB).
  - DuckDB writes the GeoParquet: the `geo` metadata is rewritten by `pipeline.to_consolidated_parquet` (version 1.1.0 + covering) with `GEOPARQUET_VERSION NONE`; keep the CRS as PROJJSON, geopandas ignores a plain `OGC:CRS84` string.
- **mapshaper.** Pinned at **0.6.59** (Docker, CI). Keep it.
- **Historical path labels.** Kept for client compatibility: `source=EXPRESS-COG-CARTO-TERRITOIRE`, `territory=metropole` for every file, `filename=raw`.

## DuckDB 1.5.5 pitfalls

- `ST_Read` (GDAL) with several threads randomly corrupts memory: segfaults, "Unsupported geometry type in WKB". Always run it with `SET threads = 1` in its own connection (see `ign.gpkg_to_parquet`, `pipeline.to_consolidated_parquet`).
- Do not load the `excel` extension in a connection that does spatial work. The TAGC xlsx is converted to parquet in a separate connection (`insee.tagc_to_parquet`).
- Run `LOAD spatial` before `SET geometry_always_xy = true`, and set it so that coordinates stay lon/lat.
- Run `SET enable_progress_bar = false`, otherwise logs are flooded.

## Commands

```bash
uv run pytest tests                                   # pipeline, no S3; mapshaper tests skipped if not on PATH
cd python-package/cartiflette && uv run pytest tests  # client; the `network` test reads a production file (read-only)
cd python-package/cartiflette && uv run pytest -m integration  # website use cases on published files (read-only); CARTIFLETTE_TEST_PATH / CARTIFLETTE_TEST_YEARS
```

- mapshaper is not installed system-wide on the dev machine. Install it locally with `npm install mapshaper@0.6.59` in a scratch directory and prepend its `node_modules/.bin` to `PATH`.
- Commit or push only when asked. Work on a branch, not on `main`: `main` is the revision the Argo workflow clones by default.
