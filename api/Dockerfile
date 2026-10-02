# Image of the GeoJSON API (api/), deployed with Argo CD.
# Build from the root of the repository: the API depends on the client in
# python-package/cartiflette.
#
#   docker build -f api/Dockerfile -t cartiflette-api .
#   docker run -p 8000:8000 cartiflette-api
FROM python:3.12-slim-bookworm

ENV \
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  PATH=/app/api/.venv/bin:$PATH

COPY --from=ghcr.io/astral-sh/uv:0.12 /uv /usr/local/bin/uv

# Non-root user: the DuckDB extensions are installed in its home
RUN useradd --uid 1000 --create-home cartiflette

# Dependencies, at the versions of api/uv.lock, then the client and the API
COPY python-package/cartiflette/ /app/python-package/cartiflette/
COPY api/pyproject.toml api/uv.lock /app/api/
COPY api/cartiflette_api/ /app/api/cartiflette_api/
RUN cd /app/api \
  && uv sync --frozen --no-dev --no-editable --no-cache \
  && chown -R cartiflette /app

USER cartiflette
WORKDIR /app/api

# DuckDB extensions, so that the API does not download them at startup
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs; INSTALL spatial; LOAD httpfs; LOAD spatial;')" \
  && python -c "import cartiflette_api; cartiflette_api.create_app()"

EXPOSE 8000
# One process per container: DuckDB already uses every core of the pod,
# scale with replicas. Behind the ingress: trust the X-Forwarded-* headers.
CMD ["uvicorn", "--factory", "cartiflette_api.app:create_app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--proxy-headers", "--forwarded-allow-ips", "*"]
