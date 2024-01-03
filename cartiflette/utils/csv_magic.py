# -*- coding: utf-8 -*-
from charset_normalizer import from_bytes
import csv
import io
import logging
import pandas as pd
from typing import Union

logger = logging.getLogger(__name__)


def magic_csv_reader(path_or_bytes: Union[str, bytes], **kwargs) -> pd.DataFrame:
    """
    Reads csv file without beforehand knowledge of separator, encoding, ...
    Further kwargs are passed to pandas read_csv method.
    Uses charset_normalizer for encoding detection and csv.Sniffer object
    for CSV format parameters.
    Any argument specifically passed to read_csv through kwargs will have
    precedence on automatic detection.

    Parameters
    ----------
    path_or_bytes : Union(str, bytes)
        Path to the CSV file or any file content
    **kwargs :
        Any argument accepted by pandas.read_csv.
        See https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

    Returns
    -------
    df : pd.DataFrame
        Read DataFrame

    """

    if "sep" in kwargs:
        # Replace by it's standard alternative returned by csv Sniffer
        kwargs["delimiter"] = kwargs.pop("sep")

    if isinstance(path_or_bytes, str):
        with open(path_or_bytes, "rb") as f:
            data = f.read()
    else:
        data = path_or_bytes.read()

    try:
        encoding = kwargs["encoding"]
    except KeyError:
        best = from_bytes(
            data,
            cp_isolation=[
                "utf-8",
                "1252",
                "iso8859_16",
                "iso8859_15",
                "iso8859_1",
                "ibm_1252",
            ],
        ).best()
        encoding = best.encoding

    sniffer = csv.Sniffer()
    if isinstance(path_or_bytes, str):
        with open(path_or_bytes, "r", encoding=encoding) as f:
            sample = f.read(4096)
    else:
        sample = data.decode(encoding)[:4096]

    dialect = sniffer.sniff(sample)

    dialect = {
        k: v
        for k, v in dialect.__dict__.items()
        if not k.startswith("_") and not k == "lineterminator"
    }

    dialect.update({"encoding": encoding})
    dialect.update(kwargs)

    logger.info(f"Reading CSV with following parameters : {dialect}")

    df = pd.read_csv(io.BytesIO(data), **dialect)
    return df
