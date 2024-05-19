from time import time

import pytest

from sprint2.asynco.exceptions import TimeOutError
from sprint2.asynco.utils import (
    async_sleep,
    gather,
    wait,
)


def test_gather_asleep():
    seconds = 1
    coros = [async_sleep(1), async_sleep(1)]

    start = time()
    gather(*coros)
    end = time() - start

    assert seconds <= end < len(coros)


def test_wait_asleep_without_timeout():
    MIN_SLEEP, MAX_SLEEP = 0.1, 0.3
    coros = [async_sleep(MIN_SLEEP), async_sleep(MAX_SLEEP)]

    start = time()
    wait(*coros, timeout=2 * MAX_SLEEP)
    end = time() - start

    assert MIN_SLEEP <= end < (MIN_SLEEP + MAX_SLEEP)


def test_wait_asleep_with_timeout():
    MIN_SLEEP, MAX_SLEEP = 0.1, 0.3
    coros = [async_sleep(MIN_SLEEP), async_sleep(MAX_SLEEP)]

    with pytest.raises(TimeOutError):
        wait(*coros, timeout=MIN_SLEEP)
