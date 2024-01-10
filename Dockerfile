FROM inseefrlab/onyxia-jupyter-python:py3.10.13

USER root

# Install mapshaper
COPY docker/install-mapshaper.sh .
RUN ./install-mapshaper.sh

# Install project Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Create structure
COPY cartiflette ./cartiflette
COPY pyproject.toml .
COPY README.md .
COPY docker/test.py .

RUN pip install .

CMD ["python", "test.py"]