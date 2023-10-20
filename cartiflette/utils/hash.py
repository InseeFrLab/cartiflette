# -*- coding: utf-8 -*-
import hashlib


def hash_file(file_path):
    """
    https://gist.github.com/mjohnsullivan/9322154
    Get the MD5 hsah value of a file
    :param file_path: path to the file for hash validation
    :type file_path:  string
    :param hash:      expected hash value of the file
    """
    m = hashlib.md5()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(1000 * 1000)  # 1MB
            if not chunk:
                break
            m.update(chunk)
    return m.hexdigest()
