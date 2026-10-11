"""HTTP helpers: a session with retries and a checked file download."""

from __future__ import annotations

import logging
import os
import zipfile

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def get_session() -> requests.Session:
    """
    Session retrying on rate limiting (the Géoplateforme allows 1 request per
    second) and server errors. Proxies are taken from http(s)_proxy env vars.
    """
    retry = Retry(
        total=8,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def download_file(url: str, dest: str, session: requests.Session) -> str:
    """Stream `url` to `dest`, checking the size against Content-Length."""
    logger.info("download %s", url)
    with session.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        expected = r.headers.get("Content-Length")
        with open(dest, "wb") as f:
            f.writelines(r.iter_content(chunk_size=1024 * 1024))
    if expected is not None and int(expected) != os.path.getsize(dest):
        os.remove(dest)
        raise OSError(f"Incomplete download of {url}")
    return dest


def download_zip(
    url: str, dest: str, session: requests.Session, attempts: int = 3
) -> str:
    """
    Download a zip archive, checked by opening it, with new attempts. The
    Insee sends no Content-Length and sometimes cuts long downloads: only
    the archive itself shows that it is truncated.
    """
    for attempt in range(1, attempts + 1):
        download_file(url, dest, session)
        try:
            with zipfile.ZipFile(dest) as z:
                if z.testzip() is None:
                    return dest
        except zipfile.BadZipFile:
            pass
        os.remove(dest)
        logger.warning("truncated archive %s (attempt %s/%s)", url, attempt, attempts)
    raise OSError(f"Incomplete download of {url} after {attempts} attempts")
