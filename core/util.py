from hashlib import sha1
from typing import Union, Tuple, Any
import functools
import logging

logger = logging.getLogger(__name__)


def hashme(s):
    return sha1(str(s).encode("utf-8")).hexdigest()


def must_one(arr):
    arr = set(arr)
    if len(arr) == 0:
        raise ValueError("Must one but is empty")
    if len(arr) > 1:
        raise ValueError("Must one but is more: "+", ".join(sorted(arr)))
    val = arr.pop()
    if val is None:
        raise ValueError("Must one but is None")
    return val


def read_file(file: str, *args, **kwargs):
    with open(file, "r") as f:
        txt = f.read().strip()
        txt = txt.format(*args, **kwargs)
        return txt


def to_set_tuple(s: Union[str, list]) -> Tuple[str]:
    if s is None:
        return tuple()
    if isinstance(s, str):
        s = s.split()
    arr = []
    for i in s:
        if i not in arr:
            arr.append(i)
    return tuple(arr)


def tp_join(t: Union[Any, Tuple]):
    if not isinstance(t, tuple):
        return t
    if len(t) == 0:
        return None
    return " ".join(map(str, t))


def logme(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"{func.__name__}()")
        return func(*args, **kwargs)
    return wrapper
