# -*- coding: utf-8 -*-
import pytest
import requests
from requests_cache import CachedSession
import logging

from tests.conftest import (
    HASH_DUMMY,
    FILESIZE_DUMMY,
    CONTENT_DUMMY,
)

from cartiflette.config import FS
from cartiflette.download.dataset import Dataset
from cartiflette.download.scraper import MasterScraper


logging.basicConfig(level=logging.INFO)


@pytest.fixture
def mock_Dataset_without_s3(monkeypatch):
    monkeypatch.setattr(Dataset, "_get_last_md5", lambda x: None)
    # monkeypatch.setattr("FS")


@pytest.fixture
def total_mock_s3(monkeypatch):
    monkeypatch.setattr(Dataset, "_get_last_md5", lambda x: None)

    def mock_unpack(self, x, validate=True):
        return {
            "downloaded": False,
            "layers": None,
            "hash": None,
            "root_cleanup": None,
        }

    monkeypatch.setattr(MasterScraper, "download_unpack", mock_unpack)
    # monkeypatch.setattr("cartiflette.THREADS_DOWNLOAD", 1)

    def mock_ls(folder):
        return [f"{folder}/md5.json"]

    monkeypatch.setattr(FS, "ls", mock_ls)


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

    monkeypatch.setattr(CachedSession, "head", mock_head)
    monkeypatch.setattr(CachedSession, "get", mock_get)


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

    monkeypatch.setattr(CachedSession, "head", mock_head)
    monkeypatch.setattr(CachedSession, "get", mock_get)


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

    monkeypatch.setattr(CachedSession, "head", mock_head)
    monkeypatch.setattr(CachedSession, "get", mock_get)
