import abc
import enum
import functools
import inspect
import typing


CoroutineType = typing.Generator


class CoroutineError(Exception):
    pass


class CoroutineStates(str, enum.Enum):
    CREATED = "CREATED"
    CANCELLED = "CANCELLED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"


class CoroutineState(abc.ABC):
    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"{cls_name}(status={self.state})"

    @property
    @abc.abstractmethod
    def state(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def cancel(self):
        raise NotImplementedError

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError

    @abc.abstractmethod
    def finish(self):
        raise NotImplementedError


class FinishedCoroutineState(CoroutineState):
    @property
    def state(self) -> str:
        return CoroutineStates.FINISHED

    def cancel(self) -> typing.NoReturn:
        msg = "the finished coroutine is uncancellable."
        raise CoroutineError(msg)

    def run(self) -> typing.NoReturn:
        msg = "the finished coroutine is unrunnable."
        raise CoroutineError(msg)

    def finish(self) -> typing.NoReturn:
        msg = "the coroutine is already finished."
        raise CoroutineError(msg)


class CancelledCoroutineState(CoroutineState):
    @property
    def state(self) -> str:
        return CoroutineStates.CANCELLED

    def cancel(self) -> typing.NoReturn:
        msg = "the coroutine is already cancelled."
        raise CoroutineError(msg)

    def run(self) -> typing.NoReturn:
        msg = "the cancelled coroutine is unrunnable."
        raise CoroutineError(msg)

    def finish(self) -> typing.NoReturn:
        msg = "the cancelled state cannot be finished."
        raise CoroutineError(msg)


class RunningCoroutineState(CoroutineState):
    @property
    def state(self) -> str:
        return CoroutineStates.RUNNING

    def cancel(self) -> CoroutineState:
        return CancelledCoroutineState()

    def run(self) -> typing.NoReturn:
        msg = "the coroutine is already running."
        raise CoroutineError(msg)

    def finish(slef) -> CoroutineState:
        return FinishedCoroutineState()


class NewCoroutineState(CoroutineState):
    @property
    def state(self) -> str:
        return CoroutineStates.CREATED

    def cancel(self) -> CoroutineState:
        return CancelledCoroutineState()

    def run(self) -> CoroutineState:
        return RunningCoroutineState()

    def finish(self) -> typing.NoReturn:
        msg = "the coroutine must be run to maybe get finished"
        raise CoroutineError(msg)


class CoroutineStateMachine:
    def __init__(self) -> None:
        self._state: CoroutineState = NewCoroutineState()

    @property
    def state(self) -> CoroutineState:
        return self._state

    def is_cancelled(self) -> bool:
        return isinstance(self._state, CancelledCoroutineState)

    def is_done(self) -> bool:
        return isinstance(self._state, FinishedCoroutineState)

    def is_running(self) -> bool:
        return isinstance(self._state, RunningCoroutineState)

    def cancel(self) -> None:
        self._state = self._state.cancel()

    def run(self) -> None:
        self._state = self._state.run()

    def finish(self) -> None:
        self._state = self._state.finish()


def _ensure_generator_function(
    gen_func: typing.Callable, *args, **kwargs
) -> CoroutineType:
    if not inspect.isgeneratorfunction(gen_func):
        msg = f"the {gen_func} is not a generator function"
        raise TypeError(msg)
    return gen_func(*args, **kwargs)


class Coroutine:
    """A custom generator-based coroutine class."""

    def __init__(
        self,
        gen_fn: typing.Callable,
        args: typing.Iterable[typing.Any],
        kwargs: typing.Mapping[str, typing.Any],
    ):
        self._coro = _ensure_generator_function(gen_fn, *args, **kwargs)
        self._res = None
        self._exc = None
        self._sm = CoroutineStateMachine()

    def exception(self) -> None | Exception:
        if self._sm.is_done():
            return self._exc
        msg = "the coroutine is not done, no exception to return"
        raise CoroutineError(msg)

    def result(self) -> typing.Any:
        if self._sm.is_done():
            return self._res
        msg = "the coroutine is not done, no result to return"
        raise CoroutineError(msg)

    @property
    def state(self) -> CoroutineState:
        return self._sm._state

    def _finalise(self) -> None:
        self._sm.finish()
        self._coro.close()

    def step(self):
        if not self._sm.is_running():
            msg = "to step, the coroutine must be run first"
            raise CoroutineError(msg)
        try:
            value = None
            while True:
                value = self._coro.send(value)
                yield
        except StopIteration as result:
            self._res = result
            self._finalise()
        except Exception as exc:
            self._exc = exc
            self._finalise()

    def run(self) -> None:
        self._sm.run()
        next(self.step())


def coroutine(gen_func: typing.Callable):
    @functools.wraps(gen_func)
    def _wrapper(*args, **kwargs):
        return Coroutine(gen_fn=gen_func, args=args, kwargs=kwargs)

    return _wrapper
