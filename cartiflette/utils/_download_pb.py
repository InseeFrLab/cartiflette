"""
Download files with nice progress bars
"""
import os
import requests
from tqdm import tqdm


def download_pb(
    url: str,
    fname: str,
    total: int = None,
    force: bool = True,
    verify: bool = True):
    """Useful function to get request with a progress bar

    Borrowed from
    https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51

    Arguments:
        url {str} -- URL for the source file
        fname {str} -- Destination where data will be written
        total {int} -- Filesize. Optional argument, recommended to let the default value.
        verify {bool} -- Optional argument, inherited from requests.get
    """

    try:
        proxies = {"http": os.environ["http_proxy"], "https": os.environ["https_proxy"]}
    except KeyError:
        proxies = {"http": "", "https": ""}

    resp = requests.get(
        url,
        proxies=proxies,
        stream=True,
        verify=verify)

    if total is None and force is False:
        total = int(resp.headers.get("content-length", 0))

    with open(fname, "wb") as file, tqdm(
        desc="Downloading: ",
        total=total,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
    ) as obj:
        for data in resp.iter_content(chunk_size=1024):
            size = file.write(data)
            obj.update(size)


def download_pb_ftp(ftp, url: str, fname: str):
    """Useful function to get request with a progress bar

    Borrowed from
    https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51

    Arguments:
        url {str} -- URL for the source file
        fname {str} -- Destination where data will be written
        ftp -- ftplib object
    """
    with open(fname, "wb") as file:
        total = ftp.size(url)
        with tqdm(
            total=total, unit_scale=True, desc=url, miniters=1, leave=False
        ) as pbar:

            def dowload_write(data):
                pbar.update(len(data))
                file.write(data)

            ftp.retrbinary(f"RETR {url}", dowload_write)
