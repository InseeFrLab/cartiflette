# -*- coding: utf-8 -*-
"""
Utils to ensure subprocess is ran with same level of debugging between windows
& linux
"""
import logging
import os
import subprocess


def run(cmd):
    if os.name == "nt":
        kwargs = {"shell": True, "text": True, "capture_output": True}
        result = subprocess.run(cmd, **kwargs)
        logging.info(result.stdout)
        if not result.returncode == 0:
            logging.warning(result.stderr)
            raise subprocess.CalledProcessError(result.returncode, cmd)
        else:
            # on windows, mapshaper's output seem to always be in stderr,
            # whether there was an error or not
            logging.info(result.stderr)

    else:
        kwargs = {
            "shell": True,
            "check": True,
            "text": True,
        }
        subprocess.run(cmd, **kwargs)

    return
