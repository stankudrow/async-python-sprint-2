import abc
import ctypes
from datetime import datetime
from functools import partial
from threading import Thread, Event
from typing import Any, Callable, Iterable, Mapping

from pydantic import BaseModel, NonNegativeInt

from sprint2.exceptions import JobError, JobFutureError
from sprint2.futures import JobFuture


class _AbstractJobThread(Thread):
    __metaclass__ = abc.ABC

    @abc.abstractmethod
    def result(self) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    def run(self) -> None:
        raise NotImplementedError


class _JobThread(_AbstractJobThread):
    def __init__(
        self,
        func: Callable,
        args: Iterable[Any],
        kwargs: Mapping[str, Any],
    ):
        super().__init__(daemon=True)
        self._func = partial(func, *args, **kwargs)
        self._fut = JobFuture()

    def result(self) -> Any:
        return self._fut.result()

    def run(self):
        try:
            res = self._func()
            self._fut.set_result(res)
        except Exception as e:
            self._fut.set_exception(e)


class _Job(_AbstractJobThread):
    def __init__(
        self,
        job_thread: _JobThread,
        stop_event: Event,
    ):
        super().__init__()
        self._job_thread = job_thread
        self._stop_event = stop_event

    # https://stackoverflow.com/questions/61630152/how-to-terminate-a-thread-in-python
    def _finalise_job_thread(self) -> None:
        try:
            self._job_thread.result()
        except JobFutureError:
            if thread_id := self._job_thread.ident:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_long(thread_id), ctypes.py_object(JobFutureError)
                )
                self._job_thread.join()

    def result(self) -> Any:
        return self._job_thread.result()

    def run(self):
        self._job_thread.start()
        while True:
            if self._stop_event.is_set():
                break
            try:
                self._job_thread.result()
                break
            except JobFutureError:
                continue
        self._finalise_job_thread()


class Job:
    job_counter: NonNegativeInt = 0

    # https://stackoverflow.com/questions/27102881/python-threading-self-stop-event-object-is-not-callable
    def __init__(
            self,
            func: Callable,
            args: Iterable[Any]=tuple(),
            kwargs: Mapping[str, Any]={},
        ):
        self._stop_event = Event()
        self._job = _Job(
            job_thread=_JobThread(func=func, args=args, kwargs=kwargs),
            stop_event=self._stop_event,
        )

    def __repr__(self) -> str:
        self.job_counter += 1
        return f"Job_{self.job_counter}"

    def result(self) -> Any:
        try:
            return self._job.result()
        except JobFutureError as e:
            raise JobError from e

    def run(self):
        self._job.start()

    def stop(self):
        self._job._stop_event.set()
        self.wait()

    def wait(self, timeout: None | float = None):
        self._job.join(timeout=timeout)


class JobInfo(BaseModel):
    start_at: None | datetime = None
    end_at: None | datetime = None
    max_retries: NonNegativeInt = 0
    dependencies: list["JobInfo"] = []
