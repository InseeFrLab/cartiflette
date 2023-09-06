# -*- coding: utf-8 -*-
"""
Created on Thu May 11 19:48:36 2023

@author: Thomas
"""
import pytest
import requests
import logging

from tests.conftest import (
    HASH_DUMMY,
    FILESIZE_DUMMY,
    CONTENT_DUMMY,
)
from cartiflette.utils import (
    create_path_bucket
)
from cartiflette.download import (
    Dataset
)

logging.basicConfig(level=logging.INFO)


@pytest.fixture
def mock_Dataset_without_s3(monkeypatch):
    monkeypatch.setattr(Dataset, "__get_last_md5__", lambda x: None)


class MockResponse:
    ok = True

    def __init__(self, success=True, content=None, *args, **kwargs):
        if not success:
            self.ok = False
        self.content = content

    def iter_content(self, chunk_size):
        content = self.content
        chunks = [
            content[i : i + chunk_size]
            for i in range(0, len(content), chunk_size)
        ]
        for chunk in chunks:
            yield chunk


class MockHttpScraper:
    def __init__(self, dict_head, success, content):
        self.dict_head = dict_head
        self.success = success
        self.content = content

    def head(self, url, *args, **kwargs):
        response = requests.Response()
        response.status_code = 200 if self.success else 404
        response.headers = self.dict_head
        return response

    def get(self, url, *args, **kwargs):
        return MockResponse(self.success, self.content)


@pytest.fixture
def mock_httpscraper_download_success(monkeypatch):
    dict_header = {
        "content-md5": HASH_DUMMY,
        "Content-length": FILESIZE_DUMMY,
    }
    success = True
    content = CONTENT_DUMMY
    mocked_session = MockHttpScraper(dict_header, success, content)

    def mock_head(self, url, *args, **kwargs):
        return mocked_session.head(url, *args, **kwargs)

    def mock_get(self, url, *args, **kwargs):
        return mocked_session.get(url, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "head", mock_head)
    monkeypatch.setattr(requests.Session, "get", mock_get)


@pytest.fixture
def mock_httpscraper_download_success_corrupt_hash(monkeypatch):
    dict_header = {
        "content-md5": HASH_DUMMY,
        "Content-length": FILESIZE_DUMMY,
    }
    success = True
    content = b"Blah Blah"
    mocked_session = MockHttpScraper(dict_header, success, content)

    def mock_head(self, url, *args, **kwargs):
        return mocked_session.head(url, *args, **kwargs)

    def mock_get(self, url, *args, **kwargs):
        return mocked_session.get(url, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "head", mock_head)
    monkeypatch.setattr(requests.Session, "get", mock_get)


@pytest.fixture
def mock_httpscraper_download_success_corrupt_length(monkeypatch):
    dict_header = {
        "Content-length": FILESIZE_DUMMY,
    }
    success = True
    content = b"Blah Blah"
    mocked_session = MockHttpScraper(dict_header, success, content)

    def mock_head(self, url, *args, **kwargs):
        return mocked_session.head(url, *args, **kwargs)

    def mock_get(self, url, *args, **kwargs):
        return mocked_session.get(url, *args, **kwargs)

    monkeypatch.setattr(requests.Session, "head", mock_head)
    monkeypatch.setattr(requests.Session, "get", mock_get)


@pytest.mark.parametrize(
    "config, expected_path",
    [
        ({"bucket": "my_bucket"}, "my_bucket/PATH_WITHIN_BUCKET/2022/administrative_level=COMMUNE/2154/region=28/vectorfile_format=geojson/provider=IGN/source=EXPRESS-COG-TERRITOIRE/raw.geojson"),
        ({"vectorfile_format": "shp"}, "BUCKET/PATH_WITHIN_BUCKET/2022/administrative_level=COMMUNE/2154/region=28/vectorfile_format=shp/provider=IGN/source=EXPRESS-COG-TERRITOIRE/"),
        ({"borders": "DEPARTEMENT", "filter_by": "REGION", "year": "2023", "value": "42", "crs": 4326}, "BUCKET/PATH_WITHIN_BUCKET/2023/administrative_level=DEPARTEMENT/4326/REGION=42/geojson/IGN/EXPRESS-COG-TERRITOIRE/raw.geojson"),
        ({"path_within_bucket": "data", "vectorfile_format": "gpkg"}, "BUCKET/data/2022/administrative_level=COMMUNE/2154/region=28/gpkg/IGN/EXPRESS-COG-TERRITOIRE/raw.gpkg"),
    ],
)
def test_create_path_bucket(config, expected_path):
    result = create_path_bucket(config)
    assert result == expected_path