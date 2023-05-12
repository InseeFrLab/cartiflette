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

logging.basicConfig(level=logging.INFO)


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
