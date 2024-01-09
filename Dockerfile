FROM python:3.11-slim-bullseye

# Install mapshaper
COPY docker/install-mapshaper.py .
RUN ./install-mapshaper.py

# Install project Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Create structure
COPY cartiflette ./cartiflette
COPY pyproject.toml .
COPY setup.py .
COPY README.md .
COPY example/download.py .

RUN pip install .

CMD ["python", "download.py"]