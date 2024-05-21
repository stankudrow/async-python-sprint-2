from time import time
from typing import Any

from sprint2.aiotools import async_sleep, gather


def coro(seconds: float, retval: Any = 0):
    yield from async_sleep(seconds)
    return retval


def test_gather():
    coros = [coro(0.02, 12), coro(0.03, 21), coro(0.01, 42)]

    start = time()
    results = gather(*coros)
    elapsed_time = time() - start

    assert results == [12, 21, 42]
    assert 0.01 <= elapsed_time < 0.035
