from collections.abc import Iterable
from itertools import cycle, islice
from typing import Any


def cycle_n(iterable: Iterable[Any], n: int) -> Iterable[Any]:
    return islice(cycle(iterable), n)
