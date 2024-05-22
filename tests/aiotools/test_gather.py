from time import time
from typing import Any

from sprint2.aiotools import async_sleep, gather


def coro(seconds: float, retval: Any = 0):
    yield from async_sleep(seconds)
    return retval


def coro_exc(seconds: float, retval: Any = 0):
    yield from async_sleep(seconds)
    raise ZeroDivisionError


def test_gather():
    coros = [coro(0.02, 12), coro(0.03, 21), coro(0.01, 42)]

    start = time()
    results = gather(*coros)
    elapsed_time = time() - start

    assert results == [12, 21, 42]
    assert 0.01 <= elapsed_time < 0.035


def test_gather_with_exceptions():
    coros = [coro(0.02, 12), coro(0.03, 21), coro_exc(0.01, 42)]

    start = time()
    results = gather(*coros, return_exceptions=True)
    elapsed_time = time() - start

    res = [r for r in results if not isinstance(r, Exception)]
    exc = [e for e in results if isinstance(e, Exception)]

    assert res == [12, 21]
    assert all([isinstance(e, ZeroDivisionError) for e in exc])

    assert 0.01 <= elapsed_time < 0.035
