FROM inseefrlab/onyxia-jupyter-python:py3.10.13

USER root

# Install mapshaper
COPY docker/install-mapshaper.sh .
RUN ./install-mapshaper.sh

# Install project Python dependencies
COPY pyproject.toml .
COPY poetry.lock .

RUN curl https://install.python-poetry.org/ | python - 
RUN poetry install --only main --no-interaction

# Create structure
COPY cartiflette ./cartiflette
COPY docker/test.py .


CMD ["python", "test.py"]