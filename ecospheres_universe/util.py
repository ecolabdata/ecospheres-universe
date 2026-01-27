import functools
import time
import unicodedata

from collections.abc import Generator
from typing import Iterable


def batched[T](sequence: list[T], n: int = 1) -> Generator[list[T]]:
    length = len(sequence)
    for ndx in range(0, length, n):
        yield sequence[ndx : min(ndx + n, length)]


def elapsed_and_count(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        t = time.time()
        val = None
        try:
            val = func(*args, **kwargs)
        finally:
            verbose_print(
                f"<{func.__name__}: count={len(val or [])}, elapsed={time.time() - t:.2f}s>"
            )
        return val

    return wrapper_decorator


def elapsed(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        t = time.time()
        try:
            val = func(*args, **kwargs)
        finally:
            verbose_print(f"<{func.__name__}: elapsed={time.time() - t:.2f}s>")
        return val

    return wrapper_decorator


def normalize_string(string: str) -> str:
    """Return NFKD-normalized, ascii-folded, lowercased form of the input string"""
    return unicodedata.normalize("NFKD", string).encode("ascii", "ignore").decode("ascii").lower()


def uniquify[T](sequence: Iterable[T]) -> list[T]:
    """Return list of unique elements from `sequence`, preserving original order"""
    return list(dict.fromkeys(sequence))


# noop unless args.verbose is set
def verbose_print(*args, **kwargs):
    return None
