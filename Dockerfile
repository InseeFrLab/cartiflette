FROM python:3.11-slim-bullseye

# Install project dependencies
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