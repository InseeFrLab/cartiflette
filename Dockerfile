FROM inseefrlab/onyxia-jupyter-python:py3.10.13

USER root

# Install mapshaper
COPY docker/install-mapshaper.sh .
RUN ./install-mapshaper.sh

# Install project Python dependencies

ENV \
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100

# Create structure
COPY pyproject.toml .
COPY uv.lock .
COPY README.md .
COPY cartiflette ./cartiflette
COPY docker/test.py .

RUN pip install uv && uv pip install -r pyproject.toml --system

CMD ["python", "test.py"]
