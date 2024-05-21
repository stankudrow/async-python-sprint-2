from collections import deque
from datetime import datetime, timedelta
from typing import Any, Generator


__all__ = [
    "async_wait",
    "wait",
]


def async_wait(*aws, timeout: None | float = None) -> Generator[Any, None, list]:
    now_ = datetime.now()
    ddl = now_ + timedelta(seconds=timeout) if timeout else None
    coros = deque(aws)
    results = []
    while coros:
        coro = coros.popleft()
        try:
            yield next(coro)
            coros.append(coro)
        except StopIteration as r:
            results.append(r.value)
        if ddl and datetime.now() > ddl:
            msg = f"the deadline ({ddl}) is exceeded"
            raise TimeoutError(msg)
    return results


def wait(*aws, timeout: None | float = None) -> list:
    waiter = async_wait(*aws, timeout=timeout)
    while True:
        try:
            next(waiter)
        except StopIteration as result:
            return result.value
