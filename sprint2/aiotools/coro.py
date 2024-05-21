from functools import wraps
from inspect import isgeneratorfunction
from typing import Any, Callable, Generator


__all__ = ["Coroutine", "coroutine"]


def validate_generator_function(gen_func: Callable) -> None:
    if not isgeneratorfunction(gen_func):
        msg = f"{gen_func} is not a generator_function"
        raise TypeError(msg)


# coroutines are not iterable, generators are.
class Coroutine:
    def __init__(self, gen_func: Callable, *args, **kwargs):
        validate_generator_function(gen_func)
        self._gen: Generator = gen_func(*args, **kwargs)

    def __next__(self) -> Any:
        return next(self._gen)


def coroutine(gen_func: Callable) -> Callable[[Any], Any]:
    @wraps(gen_func)
    def _wrapper(*args, **kwargs) -> Coroutine:
        return Coroutine(gen_func, *args, **kwargs)

    return _wrapper
