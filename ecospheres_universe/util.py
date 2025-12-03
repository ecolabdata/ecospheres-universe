import functools
import time
import yaml

from collections.abc import Generator, Sequence
from pathlib import Path
from typing import Any


def batched[T](sequence: Sequence[T], n: int = 1) -> Generator[Sequence[T]]:
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


def load_configs(*paths: Path) -> dict[str, Any]:
    conf: dict[str, Any] = {}
    for path in paths:
        conf.update(yaml.safe_load(path.read_text()))
    return conf


# noop unless args.verbose is set
def verbose_print(*args, **kwargs):
    return None
