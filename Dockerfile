FROM python:3.11-slim-bullseye

# Install mapshaper
COPY docker .
RUN ./install-mapshaper.py

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