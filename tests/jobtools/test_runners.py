from datetime import datetime, timedelta
from time import sleep

import pytest

from sprint2.aiotools import wait
from sprint2.jobtools.job import Job
from sprint2.jobtools.runners import async_run_job


NOW = datetime.now()


def _fn(*args, **kwargs):
    suffix = f"ARGs = {args!r}; KWARGs={kwargs!r}"
    to_sleep = 0.001
    sleep(to_sleep)
    print(f"Intermezzo: slept for {to_sleep} s -> {suffix}")
    largs = len(args) + len(kwargs)
    return largs


def test_run_job():
    job1 = Job(
        fn=_fn,
        args=(1, 2),
        start=NOW + timedelta(seconds=2),
        dependencies=[Job(fn=_fn, kwargs={"a": "b"})],
    )
    job2 = Job(fn=_fn, args=[1], dependencies=[Job(fn=_fn, kwargs={"c": "d"})])
    job_runners = [async_run_job(job1), async_run_job(job2)]

    results = wait(*job_runners)

    assert results == [1, 2]


def test_run_job_child_timeouted():
    job1 = Job(
        fn=_fn,
        args=(1, 2),
        start=NOW + timedelta(seconds=2),
        dependencies=[Job(fn=_fn, kwargs={"a": "b"}, duration=0)],
    )
    job_runners = [async_run_job(job1)]

    with pytest.raises(TimeoutError):
        wait(*job_runners)
