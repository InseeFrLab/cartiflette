# Image of the production pipeline, used by argo-pipeline/pipeline.yaml.
# It holds the tools and dependencies only: Argo clones the code of the
# pipeline (parameter `revision`) and runs it with PYTHONPATH=/mnt/bin.
FROM python:3.12-slim-bookworm

ENV \
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random

# mapshaper (Node.js), same version as in the CI
COPY docker/install-mapshaper.sh /tmp/
RUN /tmp/install-mapshaper.sh && rm /tmp/install-mapshaper.sh

# Python dependencies, at the versions of uv.lock
COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /tmp/pipeline/
RUN cd /tmp/pipeline \
  && uv export --frozen --no-dev --no-emit-project --no-hashes -o requirements.txt \
  && uv pip install --system --no-cache -r requirements.txt \
  && rm -rf /tmp/pipeline

# DuckDB extensions, so that the pipeline does not download them at runtime
# (installed for root, the user the Argo steps run as)
RUN python -c "import duckdb; duckdb.sql('INSTALL spatial; INSTALL excel;')" \
  && python -c "import duckdb, py7zr, requests, s3fs; duckdb.sql('LOAD spatial; LOAD excel;')" \
  && mapshaper -v
