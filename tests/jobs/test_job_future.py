import pytest

from sprint2.exceptions import JobFutureError
from sprint2.futures import JobFuture


def test_job_future_result() -> None:
    fut = JobFuture()

    with pytest.raises(JobFutureError):
        fut.result()

    fut.set_result(None)

    assert fut.result() is None


def test_job_future_exception() -> None:
    fut = JobFuture()

    fut.exception()

    with pytest.raises(TypeError):
        fut.set_exception(5)

    exc_type = ZeroDivisionError
    exc_obj = exc_type("test")
    fut.set_exception(exc_obj)
    with pytest.raises(exc_type):
        fut.exception()

    with pytest.raises(exc_type):
        fut.set_result("some value")
        fut.result()
