from contextlib import nullcontext as does_not_raise, suppress
from typing import Any, Callable, ContextManager

import pytest

from sprint2.coro import Coroutine, CoroutineError


CORO_NOT_DONE_RESULT = "the coroutine is not done, no result to return"
CORO_NOT_DONE_EXCEPTION = "the coroutine is not done, no exception to return"
CORO_DONE_IS_NO_RUN = "the finished coroutine is unrunnable"


def _foo():
    return None


def _gen1():
    yield 1
    return 2


def _gen2():
    yield 42
    yield 21
    return 12


@pytest.mark.parametrize(
    ("func", "exp"), [(_foo, pytest.raises(CoroutineError)), (_gen1, does_not_raise())]
)
def test_is_coroutine(func: Callable, exp: ContextManager):
    with exp:
        Coroutine(func)


def test_undone_results():
    coro = Coroutine(gen_fn=_gen1)

    with pytest.raises(CoroutineError, match=CORO_NOT_DONE_RESULT):
        coro.result()

    with pytest.raises(CoroutineError, match=CORO_NOT_DONE_EXCEPTION):
        coro.exception()


@pytest.mark.parametrize(
    ("gen", "result", "exception"),
    [
        (_gen1, 2, None),
        (_gen2, 12, None),
    ],
)
def test_wait(gen: Callable, result: Any, exception: None | Exception):
    coro = Coroutine(gen_fn=gen)

    val = coro.wait()

    assert val is None
    assert coro.is_done()
    assert coro.result() == result

    if exception is None:
        assert coro.exception() is None
    else:
        assert coro.exception() == exception


def test_next_operator():
    coro = Coroutine(gen_fn=_gen1)

    val = next(coro)
    assert val == 1
    assert coro.is_running()

    with suppress(StopIteration):
        val = next(coro)
    assert val == 1  # the previous yield
    assert coro.is_done()

    assert coro.result() == 2


def test_coroutine_switch():
    coro1 = Coroutine(gen_fn=_gen1)
    coro2 = Coroutine(gen_fn=_gen2)
    yields = []

    yields.append(next(coro1))
    yields.append(next(coro2))
    assert yields == [1, 42]
    assert coro1.state.is_running()
    assert coro2.state.is_running()

    yields.append(next(coro2))
    with suppress(StopIteration):
        next(coro1)
    assert coro2.state.is_running()
    assert yields == [1, 42, 21]
    assert coro1.state.is_done()
    assert coro1.result() == 2

    with suppress(StopIteration):
        next(coro2)
    assert yields == [1, 42, 21]
    assert coro1.result() == 2
    assert coro2.result() == 12
    assert coro1.state.is_done()
    assert coro2.state.is_done()

    with pytest.raises(CoroutineError, match=CORO_DONE_IS_NO_RUN):
        next(coro1)
    with pytest.raises(CoroutineError, match=CORO_DONE_IS_NO_RUN):
        next(coro2)
