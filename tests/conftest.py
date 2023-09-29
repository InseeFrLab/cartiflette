# -*- coding: utf-8 -*-

import os
import shutil

DIR = os.path.join(os.path.dirname(__file__), "data")

DUMMY_FILE_1 = os.path.join(os.path.dirname(__file__), "data", "dummy1.txt")
CONTENT_DUMMY = b"Dummy"
HASH_DUMMY = "bcf036b6f33e182d4705f4f5b1af13ac"
FILESIZE_DUMMY = 4

DUMMY_FILE_2 = os.path.join(os.path.dirname(__file__), "data", "dummy2.txt")


def pytest_sessionstart(session):
    try:
        os.makedirs(DIR)
    except Exception:
        pass

    with open(DUMMY_FILE_1, "wb") as f:
        f.write(CONTENT_DUMMY)

    with open(DUMMY_FILE_2, "w") as f:
        for x in range(2):
            f.write("Blah")


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(DIR)
