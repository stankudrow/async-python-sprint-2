from contextlib import nullcontext as does_not_raise, suppress
from typing import Any, Callable, ContextManager

import pytest

from sprint2.asynco.coro import Coroutine, CoroutineError, CoroutineStatuses
from sprint2.asynco.utils import wait


CORO_DONE_IS_NO_RUN = "the finished coroutine is unrunnable"


def _foo():
    return 100


def _gen1():
    yield 1
    return 2


def _gen2():
    yield 42
    yield 21
    return 12


def _gen3():
    yield 12
    raise ZeroDivisionError("oh no")


@pytest.mark.parametrize(
    ("func", "exp"), [(_foo, pytest.raises(CoroutineError)), (_gen1, does_not_raise())]
)
def test_is_coroutine(func: Callable, exp: ContextManager):
    with exp:
        Coroutine(func)


@pytest.mark.parametrize(
    ("gen", "result", "expectation"),
    [
        (_gen1, 2, does_not_raise()),
        (_gen2, 12, does_not_raise()),
        (_gen3, None, pytest.raises(ZeroDivisionError)),
    ],
)
def test_wait(gen: Callable, result: Any, expectation: ContextManager):
    coro = Coroutine(gen_fn=gen)

    with expectation:
        res = wait(coro)
        assert res == [result]


def test_next_with_normal_return():
    yieldo = 1
    coro = Coroutine(gen_fn=_gen1)

    val = next(coro)
    assert val == yieldo

    with suppress(StopIteration):
        val = next(coro)
    assert val == yieldo


def test_next_with_exception_raised():
    yieldo = 12
    coro = Coroutine(gen_fn=_gen3)

    val = next(coro)
    assert val == yieldo

    with suppress(StopIteration, ZeroDivisionError):
        val = next(coro)
    assert val == yieldo


def test_coroutine_switch():
    coro1 = Coroutine(gen_fn=_gen1)
    coro2 = Coroutine(gen_fn=_gen2)
    yields = []

    yields.append(next(coro1))
    yields.append(next(coro2))
    assert yields == [1, 42]

    yields.append(next(coro2))
    with suppress(StopIteration):
        next(coro1)
    assert yields == [1, 42, 21]

    with suppress(StopIteration):
        next(coro2)
    assert yields == [1, 42, 21]

    with pytest.raises(CoroutineError, match=CORO_DONE_IS_NO_RUN):
        next(coro1)
    with pytest.raises(CoroutineError, match=CORO_DONE_IS_NO_RUN):
        next(coro2)


def test_coro_repr_normal_return():
    coro = Coroutine(gen_fn=_gen1)

    cls_name = Coroutine.__name__
    repr_base = f"{cls_name}(id={id(coro)}"

    assert str(coro) == f"{repr_base}, status={CoroutineStatuses.CREATED})"

    next(coro)
    assert str(coro) == f"{repr_base}, status={CoroutineStatuses.RUNNING})"

    with suppress(StopIteration):
        next(coro)
    assert str(coro) == f"{repr_base}, status={CoroutineStatuses.FINISHED})"


def test_repr_coro_exception_raised():
    coro = Coroutine(gen_fn=_gen3)

    cls_name = Coroutine.__name__
    repr_base = f"{cls_name}(id={id(coro)}"

    assert str(coro) == f"{repr_base}, status={CoroutineStatuses.CREATED})"

    next(coro)
    assert str(coro) == f"{repr_base}, status={CoroutineStatuses.RUNNING})"

    with suppress(StopIteration, ZeroDivisionError):
        next(coro)
    assert str(coro) == f"{repr_base}, status={CoroutineStatuses.FINISHED})"
