from collections import deque
from datetime import datetime, timedelta
from typing import Any, Generator

from sprint2.asynco.coro import coroutine
from sprint2.asynco.exceptions import TimeOutError


def _validate_timeout(seconds: float) -> None:
    if seconds < 0:
        msg = f"cannot sleep negative seconds ({seconds})"
        raise ValueError(msg)


def _wait_coro(coro) -> Any:
    while True:
        try:
            next(coro)
        except StopIteration as result:
            return result.value


def get_deadline(seconds: None | float) -> None | datetime:
    now_ = datetime.now()
    if seconds is None:
        return None
    _validate_timeout(seconds)
    return now_ + timedelta(seconds=seconds)


@coroutine
def async_gather(*coros, exceptions: bool = False) -> Generator[None, None, list]:
    aws = deque(coros)
    results: list = []
    while aws:
        aw = aws.popleft()
        try:
            next(aw)
            aws.append(aw)
            yield
        except StopIteration as result:
            results.append(result.value)
        except Exception as exc:
            if not exceptions:
                raise exc
            results.append(exc)
    return results


@coroutine
def async_sleep(seconds: float) -> Generator[None, None, float]:
    """Sleep for non-negative seconds and return them."""

    _validate_timeout(seconds)
    now_ = datetime.now()
    deadline = now_ + timedelta(seconds=seconds)
    while datetime.now() < deadline:
        yield
    return seconds


@coroutine
def async_wait(*coros, timeout: None | float = None) -> Generator[None, None, Any]:
    deadline = get_deadline(seconds=timeout)
    aws = deque(coros)
    results: list = []
    while aws:
        if deadline and datetime.now() > deadline:
            msg = "the timeout for waiting is exceeded"
            raise TimeOutError(msg)
        aw = aws.popleft()
        try:
            next(aw)
            aws.append(aw)
            yield
        except StopIteration as result:
            results.append(result.value)
    return results


def gather(*coros, exceptions: bool = False) -> list:
    return _wait_coro(async_gather(*coros, exceptions=exceptions))


def wait(*coros, timeout: None | float = None) -> Any:
    return _wait_coro(async_wait(*coros, timeout=timeout))
