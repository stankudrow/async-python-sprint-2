from collections import deque
from dataclasses import dataclass
from typing import Any, Generator, Iterator

from sprint2.aiotools.wait import wait


__all__ = ["async_gather", "gather"]


@dataclass
class _NAW:
    num: int
    aw: Iterator
    res: Any = None


def async_gather(*aws, return_exceptions: bool = False) -> Generator[Any, None, Any]:
    coros = deque([_NAW(num=num, aw=aw) for num, aw in enumerate(aws)])
    results: list[_NAW] = []
    while coros:
        coro = coros.popleft()
        try:
            yield next(coro.aw)
            coros.append(coro)
        except StopIteration as r:
            coro.res = r.value
            results.append(coro)
        except Exception as exc:
            if not return_exceptions:
                raise exc
            coro.res = exc
            results.append(coro)
    return [naw.res for naw in sorted(results, key=lambda _naw: _naw.num)]


def gather(*aws, return_exceptions: bool = False) -> list:
    gatherer = async_gather(*aws, return_exceptions=return_exceptions)
    results = wait(gatherer)
    return results.pop()
