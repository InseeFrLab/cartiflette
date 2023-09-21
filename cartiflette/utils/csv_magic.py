# -*- coding: utf-8 -*-
from charset_normalizer import detect
import csv
import io
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def magic_csv_reader(path: str, **kwargs) -> pd.DataFrame:
    """
    Reads csv file without beforehand knowledge of separator, encoding, ...
    Further kwargs are passed to pandas read_csv method.
    Uses charset_normalizer for encoding detection and csv.Sniffer object
    for CSV format parameters.
    Any argument specifically passed to read_csv through kwargs will have
    precedence on automatic detection.

    Parameters
    ----------
    path : str
        Path to the CSV file.
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

    with open(path, "rb") as f:
        data = f.read()
    try:
        encoding = kwargs["encoding"]
    except KeyError:
        detection = detect(data)
        encoding = detection["encoding"]

    sniffer = csv.Sniffer()
    with open(path, "r", encoding=encoding) as f:
        sample = f.read(4096)
    dialect = sniffer.sniff(sample)

    dialect = {
        k: v
        for k, v in dialect.__dict__.items()
        if not k.startswith("_") and not k == "lineterminator"
    }

    dialect.update({"encoding": encoding})
    dialect.update(kwargs)

    logger.warning(f"Reading CSV with following parameters : {dialect}")

    df = pd.read_csv(io.BytesIO(data), **dialect)
    return df
