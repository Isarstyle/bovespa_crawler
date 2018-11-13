# -*- coding: utf-8 -*
import re
import pickle
from pathlib import Path

from dateutil.parser import parse as date_parse


def mk_datetime(datetime_str):
    """
    Process ISO 8661 date time formats https://en.wikipedia.org/wiki/ISO_8601
    """
    return date_parse(datetime_str)


def get_control_file(filename, default=None):
    file = Path(filename)
    if file.exists():
        with open(filename, "rb") as f:
            return pickle.load(f, encoding="utf-8")

    return default


def put_control_file(filename, content):
    with open(filename, "wb") as f:
        return pickle.dump(content, f, pickle.HIGHEST_PROTOCOL)


def get_cache_folder(cache_folder, extra_path=None):
    if not cache_folder:
        import os
        cache_folder = os.path.dirname(os.path.realpath(__file__))

    if extra_path:
        cache_folder = "{path}/{extra_path}".format(
            path=cache_folder, extra_path=extra_path)

    Path(cache_folder).mkdir(parents=True, exist_ok=True)

    return cache_folder