FROM inseefrlab/onyxia-jupyter-python:py3.10.13

USER root

# Install mapshaper
COPY docker/install-mapshaper.sh .
RUN ./install-mapshaper.sh

# Install project Python dependencies
COPY pyproject.toml
COPY poetry.lock

RUN curl https://install.python-poetry.org/ | python - 
RUN poetry install --only main

# Create structure
COPY cartiflette ./cartiflette
COPY pyproject.toml .
COPY README.md .
COPY docker/test.py .

RUN pip install .

CMD ["python", "test.py"]
