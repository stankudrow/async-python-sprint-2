import abc
import enum
import functools
import inspect
import typing


CoroutineType = typing.Generator


class CoroutineError(Exception):
    pass


class CoroutineStatuses(str, enum.Enum):
    CREATED = "CREATED"
    CANCELLED = "CANCELLED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"


class CoroutineState(abc.ABC):
    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"{cls_name}(status={self.status})"

    @abc.abstractmethod
    def is_done(self) -> bool:
        return False

    @abc.abstractmethod
    def is_running(self) -> bool:
        return False

    @property
    @abc.abstractmethod
    def status(self) -> str:
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
    def status(self) -> str:
        return CoroutineStatuses.FINISHED

    def is_done(self) -> bool:
        return True

    def is_running(self) -> bool:
        return False

    def cancel(self) -> typing.NoReturn:
        msg = "the finished coroutine is uncancellable"
        raise CoroutineError(msg)

    def run(self) -> typing.NoReturn:
        msg = "the finished coroutine is unrunnable"
        raise CoroutineError(msg)

    def finish(self) -> typing.NoReturn:
        msg = "the coroutine is already finished"
        raise CoroutineError(msg)


class CancelledCoroutineState(CoroutineState):
    @property
    def status(self) -> str:
        return CoroutineStatuses.CANCELLED

    def is_done(self) -> bool:
        return False

    def is_running(self) -> bool:
        return False

    def cancel(self) -> typing.NoReturn:
        msg = "the coroutine is already cancelled"
        raise CoroutineError(msg)

    def run(self) -> typing.NoReturn:
        msg = "the cancelled coroutine is unrunnable"
        raise CoroutineError(msg)

    def finish(self) -> typing.NoReturn:
        msg = "the cancelled state cannot be finished"
        raise CoroutineError(msg)


class RunningCoroutineState(CoroutineState):
    @property
    def status(self) -> str:
        return CoroutineStatuses.RUNNING

    def is_done(self) -> bool:
        return False

    def is_running(self) -> bool:
        return True

    def cancel(self) -> CoroutineState:
        return CancelledCoroutineState()

    def run(self) -> typing.NoReturn:
        msg = "the coroutine is already running"
        raise CoroutineError(msg)

    def finish(slef) -> CoroutineState:
        return FinishedCoroutineState()


class NewCoroutineState(CoroutineState):
    @property
    def status(self) -> str:
        return CoroutineStatuses.CREATED

    def is_done(self) -> bool:
        return False

    def is_running(self) -> bool:
        return False

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
        return self._state.is_done()

    def is_running(self) -> bool:
        return self._state.is_running()

    def cancel(self) -> None:
        self._state = self._state.cancel()

    def run(self) -> None:
        self._state = self._state.run()

    def finish(self) -> None:
        self._state = self._state.finish()


def _ensure_generator(gen_func: typing.Callable, *args, **kwargs) -> CoroutineType:
    if not inspect.isgeneratorfunction(gen_func):
        msg = f"the {gen_func} is not a generator function"
        raise TypeError(msg)
    iter(args)
    iter(kwargs)
    return gen_func(*args, **kwargs)


class Coroutine:
    """A custom generator-based coroutine class.

    A coroutine accepts a generator function only.

    When the coroutine is instantiated, it has the CREATED state.
    The `wait` method allows to wait for the result of a coroutine.
    Otherwise the coroutine can be finished via successive `next` calls.

    The coroutine can be finished in two ways:
    1. normally - with a result and no exception
    2. otherwise with an exception and None result

    In the first way, the `result` method will return some result.
    Otherwise, when an exception occured, the latter is raised.
    The `exception` method allows to get the occured exception safely.
    """

    def __init__(
        self,
        gen_fn: typing.Callable,
        args: None | typing.Iterable[typing.Any] = None,
        kwargs: None | typing.Mapping[str, typing.Any] = None,
    ):
        args = args if args else ()
        kwargs = kwargs if kwargs else {}
        try:
            self._coro = _ensure_generator(gen_fn, *args, **kwargs)
        except TypeError as e:
            raise CoroutineError(str(e)) from e
        self._res: typing.Any = None
        self._exc: None | Exception = None
        self._sm = CoroutineStateMachine()

    def __next__(self) -> typing.Any:
        if not self.is_running():
            self._run()
        return next(self._step())

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        status = self.state.status
        prefix = f"{cls_name}(id={id(self)}, status={status}"
        if self.is_done():
            if exc := self.exception():
                prefix = f"{prefix}, raised={exc}"
            else:
                prefix = f"{prefix}, returned={self.result()}"
        return f"{prefix})"

    @property
    def state(self) -> CoroutineState:
        return self._sm._state

    def is_done(self) -> bool:
        return self.state.is_done()

    def is_running(self) -> bool:
        return self.state.is_running()

    def exception(self) -> None | Exception:
        """Returns an exception or None for a done coroutine.

        Raises:
            CoroutineError - if a coroutine is not done

        Returns:
            None - if a coroutine is finished normally
            Exception - otherwise
        """

        if self.state.is_done():
            return self._exc
        msg = "the coroutine is not done, no exception to return"
        raise CoroutineError(msg)

    def result(self) -> typing.Any:
        """Returns either the result or raises an exception.

        Raises:
            CoroutineError - accessing the result of an unfinished coroutine
        """

        if self.state.is_done():
            if exc := self.exception():
                raise exc
            return self._res
        msg = "the coroutine is not done, no result to return"
        raise CoroutineError(msg)

    def _finalise(self) -> None:
        self._sm.finish()
        self._coro.close()

    def _run(self) -> None:
        """Moves the coroutine into the running state.

        To proceed, use the next function on the coroutine.
        """

        self._sm.run()

    def _step(self) -> typing.Generator[typing.Any, None, None]:
        if not self.state.is_running():
            msg = "the coroutine is not running"
            raise CoroutineError(msg)
        try:
            value = None
            while True:
                value = self._coro.send(value)
                yield value
        except StopIteration as result:
            self._res = result.value
            self._finalise()
        except Exception as exc:
            self._exc = exc
            self._finalise()

    def wait(self) -> None:
        """Waits for the coroutine.

        The waiting is done in a blocking way.
        To get the result, use the result() function.
        """

        if not self.state.is_running():
            self._run()
        for _ in self._step():
            pass


def coroutine(gen_func: typing.Callable):
    @functools.wraps(gen_func)
    def _wrapper(*args, **kwargs):
        return Coroutine(gen_fn=gen_func, args=args, kwargs=kwargs)

    return _wrapper
