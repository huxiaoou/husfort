import os
import shutil
import re
import functools
import datetime as dt
from itertools import islice
from loguru import logger
from typing import Any


def get_mix_string_len(mix_string: str, expected_len: int):
    """

    :param mix_string: example "食品ETF09"
    :param expected_len: length of expected output string
    :return: f"{mix_string:ks}" would occupy expected_len characters when print,
             which will make mix_string aligned with pure English string
    """
    # chs_string = re.sub("[0-9a-zA-Z]", "", t_mix_string)
    chs_string = re.sub("[\\da-zA-Z]", "", mix_string)
    chs_string_len = len(chs_string)
    k = max(expected_len - chs_string_len, len(mix_string) + chs_string_len)
    return k


def SetFontColor(c):
    def inner(s: str | int | float | dt.datetime | Any):
        """

        :param s: Any classes with __format__ or __str__ or __repr__ method defined
        :return:
        """
        return f"\033[{c}m{s}\033[0m"

    return inner


SFR = SetFontColor(c="0;31;40")  # Red
SFG = SetFontColor(c="0;32;40")  # Green
SFY = SetFontColor(c="0;33;40")  # Yellow
SFB = SetFontColor(c="0;34;40")  # Blue
SFM = SetFontColor(c="0;35;40")  # Magenta
SFC = SetFontColor(c="0;36;40")  # Cyan
SFW = SetFontColor(c="0;37;40")  # White


def check_and_mkdir(dir_path: str, verbose: bool = False):
    try:
        os.mkdir(dir_path)
    except FileExistsError:
        pass
    if verbose:
        logger.info(f"[INF] Making directory {SFG(dir_path)}")
    return 0


def check_and_makedirs(dir_path: str, verbose: bool = False):
    try:
        os.makedirs(dir_path)
    except FileExistsError:
        pass
    if verbose:
        logger.info(f"[INF] Making directory {SFG(dir_path)}")
    return 0


def remove_files_in_the_dir(dir_path: str):
    for f in os.listdir(dir_path):
        os.remove(os.path.join(dir_path, f))
    return 0


def check_and_remove_tree(dir_path: str):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    return 0


def qtimer(func):
    # This function shows the execution time of
    # the function object passed
    @functools.wraps(func)  # use this statement to make this function compatible within custom classes
    def wrap_func(*args, **kwargs):
        t1 = dt.datetime.now()
        logger.info(f"Function {SFG(f'{func.__name__!r}')} begins")
        result = func(*args, **kwargs)
        t2 = dt.datetime.now()
        duration = (t2 - t1).total_seconds()
        logger.info(f"Function {SFG(f'{func.__name__!r}')} ends. Executed in {SFG(f'{duration:.4f}')} seconds")
        return result

    return wrap_func


def hide_cursor():
    print("\033[?25l", end="")
    return 0


def show_cursor():
    print("\033[?25h", end="")
    return 0


def error_handler(error):
    logger.error(error)


def batched(iterable, batch_size: int):
    i = iter(iterable)
    piece = list(islice(i, batch_size))
    while piece:
        yield piece
        piece = list(islice(i, batch_size))


if __name__ == "__main__":
    w = 24
    test_string = "食品09ETF"

    h = "-" * w
    adj_w = get_mix_string_len(mix_string=test_string, expected_len=w)
    print(h)
    print("NOT ALIGNED:")
    print(f"{test_string:>{f'{w}s'}}")
    print(h)
    print("ALIGNED:")
    print(f"{test_string:>{f'{adj_w}s'}}")
    print(h)
