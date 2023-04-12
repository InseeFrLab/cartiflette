FROM python:3.11-slim-bullseye

# Install project dependencies
COPY requirements.txt .
COPY example/download.py .
RUN pip install -r requirements.txt
RUN pip install .

CMD ["python", "download.py"]