from collections import deque
from dataclasses import dataclass
from typing import Any, Iterator


__all__ = ["gather"]


@dataclass
class _NAW:
    num: int
    aw: Iterator
    res: Any = None


def gather(*aws, return_exceptions: bool = False) -> list:
    coros = deque([_NAW(num=num, aw=aw) for num ,aw in enumerate(aws)])
    results: list[_NAW] = []
    while coros:
        coro = coros.popleft()
        try:
            next(coro.aw)
            coros.append(coro)
        except StopIteration as r:
            coro.res = r.value
            results.append(coro)
        except Exception as exc:
            if not return_exceptions:
                raise exc
            results.append(exc)
    return [naw.res for naw in sorted(results, key=lambda _naw: _naw.num)]
