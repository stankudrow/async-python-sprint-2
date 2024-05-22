from contextlib import nullcontext as does_not_raise
from typing import Callable, ContextManager

import pytest

from sprint2.aiotools import coroutine, wait, async_sleep, Coroutine


def _fn(*args, **kwargs):
    return len(args) + len(kwargs)


def _gen(*args, **kwargs):
    suffix = f"args={args!r}, kwargs={kwargs!r}"
    print(f"hello: {suffix}")
    yield from async_sleep(0.01)
    result = len(args) + len(kwargs)
    print(f"bye: {suffix}")
    return result


def _async_gen(coro: Coroutine):
    yield from coro  # type: ignore


@pytest.mark.parametrize(
    ("callable_", "expectation"),
    [
        (_fn, pytest.raises(TypeError)),
        (_gen, does_not_raise()),
    ],
)
def test_is_coroutine(callable_: Callable, expectation: ContextManager):
    with expectation:
        coroutine(callable_)()  # type: ignore


def test_is_coroutine_iterable():
    coro = coroutine(_gen)()
    agen = _async_gen(coro)
    with pytest.raises(TypeError):
        next(agen)


def test_wait_coroutine():
    coro1 = coroutine(_gen)(1, 2, **{"3": 4, "a": "b"})
    coro2 = coroutine(_gen)(1, 2, lol="kek")

    results = wait(coro2, coro1)

    assert results == [3, 4]
