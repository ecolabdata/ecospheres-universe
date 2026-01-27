import functools
import time
import unicodedata

from collections.abc import Generator
from types import FunctionType
from typing import Any, Callable, Iterable, Mapping


# weak mapping to avoid having to cast nested json
JSONObject = Mapping[str, Any]


def batched[T](sequence: list[T], n: int = 1) -> Generator[list[T]]:
    length = len(sequence)
    for ndx in range(0, length, n):
        yield sequence[ndx : min(ndx + n, length)]


def elapsed_and_count[T, **P](func: Callable[P, T]) -> Callable[P, T]:
    # https://docs.astral.sh/ty/reference/typing-faq/#why-does-ty-say-callable-has-no-attribute-__name__
    assert isinstance(func, FunctionType)

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


def elapsed[T, **P](func: Callable[P, T]) -> Callable[P, T]:
    assert isinstance(func, FunctionType)

    @functools.wraps(func)
    def wrapper_decorator(*args: P.args, **kwargs: P.kwargs) -> T:
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
