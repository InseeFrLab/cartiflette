FROM inseefrlab/onyxia-jupyter-python:py3.10.13

USER root

# mapshaper (Node.js)
COPY docker/install-mapshaper.sh .
RUN ./install-mapshaper.sh

ENV \
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100

COPY pyproject.toml uv.lock README.md ./
COPY cartiflette ./cartiflette

RUN pip install uv && uv pip install -r pyproject.toml --system

# DuckDB extensions, so that the pipeline does not download them at runtime
RUN python -c "import duckdb; duckdb.sql('INSTALL spatial; INSTALL excel;')"
