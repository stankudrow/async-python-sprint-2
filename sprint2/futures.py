from typing import Any

from sprint2.exceptions import JobFutureError


class JobFuture:
    """Represents the promise of a job."""

    def __init__(self):
        self._exc: None | Exception = None
        self._is_done: bool = False
        self._result: Any = None

    @property
    def is_done(self) -> bool:
        return self._is_done

    def exception(self) -> None:
        if exc := self._exc:
            raise exc
        return None

    def result(self) -> Any:
        if not self.is_done:
            msg = f"The job {self} is not done"
            raise JobFutureError(msg)
        self.exception()
        return self._result

    def set_exception(self, exc: Exception) -> None:
        if not isinstance(exc, BaseException):
            msg = f"the {exc!r} is not of exception type"
            raise TypeError(msg)
        self._exc = exc

    def set_result(self, value: Any) -> None:
        self._is_done = True
        self._result = value
