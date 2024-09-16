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
  PIP_DEFAULT_TIMEOUT=100 \
  # Poetry's configuration:
  POETRY_NO_INTERACTION=1 \
  POETRY_VIRTUALENVS_CREATE=false \
  POETRY_CACHE_DIR='/var/cache/pypoetry' \
  POETRY_HOME='/usr/local'

COPY pyproject.toml .
COPY poetry.lock .
COPY README.md .

RUN curl https://install.python-poetry.org/ | python - 
RUN poetry install --only main --no-interaction

# Create structure
COPY cartiflette ./cartiflette
COPY docker/test.py .


CMD ["python", "test.py"]