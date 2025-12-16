import functools
import time

from collections.abc import Generator


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


# noop unless args.verbose is set
def verbose_print(*args, **kwargs):
    return None
