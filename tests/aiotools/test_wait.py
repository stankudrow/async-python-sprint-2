from time import time
from typing import Any

import pytest

from sprint2.aiotools import async_sleep, wait


def coro(seconds: float, retval: Any = 0):
    yield from async_sleep(seconds)
    return retval


def test_wait():
    coros = [coro(0.02, 12), coro(0.03, 21), coro(0.01, 42)]

    start = time()
    results = wait(*coros)
    elapsed_time = time() - start

    assert results == [42, 12, 21]
    assert 0.01 <= elapsed_time < 0.035


def test_wait_with_timeout():
    coros = [coro(0.02, 12), coro(0.03, 21), coro(0.01, 42)]

    start = time()
    with pytest.raises(TimeoutError):
        wait(*coros, timeout=0.01)
    elapsed_time = time() - start

    assert 0.01 <= elapsed_time < 0.03
