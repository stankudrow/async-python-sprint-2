from threading import active_count
from time import sleep

import pytest

from sprint2.exceptions import JobError
from sprint2.jobs import Job

SLEEP_DEFAULT: float = 0.001 * 10


def _assert_one_active_thread():
    __tracebackhide__ = True

    assert active_count() == 1


def _sleep_finitely(seconds: float = SLEEP_DEFAULT):
    sleep(seconds)
    return seconds


def _sleep_infinitely(seconds: float = SLEEP_DEFAULT):
    while True:
        sleep(seconds)


def test_job_run_wait_on_finite_func() -> None:
    job = Job(func=sum, args=([0, -1, 2, -3, 4],))

    job.run()
    job.wait(timeout=SLEEP_DEFAULT)

    _assert_one_active_thread()
    assert job.result() == 2


def test_job_run_stop_on_infinite_func() -> None:
    job = Job(func=_sleep_infinitely, args=(SLEEP_DEFAULT * 10,))

    job.run()
    sleep(SLEEP_DEFAULT)
    job.stop()

    _assert_one_active_thread()
    with pytest.raises(JobError):
        job.result()


def test_job_stop_before_result() -> None:
    job = Job(func=_sleep_finitely, args=(SLEEP_DEFAULT * 5,))

    job.run()
    sleep(SLEEP_DEFAULT)
    job.stop()

    _assert_one_active_thread()
    with pytest.raises(JobError):
        job.result()


def test_job_stop_after_result() -> None:
    fut_result = SLEEP_DEFAULT
    job = Job(func=_sleep_finitely, args=(fut_result,))

    job.run()
    sleep(SLEEP_DEFAULT * 5)
    job.stop()

    _assert_one_active_thread()
    assert job.result() == fut_result
