from datetime import datetime, timedelta
from time import sleep

import pytest

from sprint2.jobtools import Job
from sprint2.scheduler import Scheduler, SchedulerError


NOW = datetime.now()


def _fn(*args, **kwargs):
    sleep(0.0005)
    result = len(args) + len(kwargs)
    print(f"_fn -> Bye with {result}")
    return result


@pytest.fixture()
def sched1() -> Scheduler:
    sched = Scheduler(pool_size=1)

    sched.push(
        Job(
            fn=_fn,
            start=NOW + timedelta(seconds=1),
            dependencies=[
                Job(
                    fn=_fn,
                    duration=0,
                ),
                Job(fn=_fn, start=NOW - timedelta(seconds=1)),
            ],
        )
    )
    sched.push(
        Job(
            fn=_fn,
            args=[1],
            kwargs={"a": "b"},
            start=NOW + timedelta(seconds=1),
            dependencies=[
                Job(
                    fn=_fn,
                    kwargs={"b": "v"},
                    duration=1,
                ),
                Job(fn=_fn, args=(2, 3), start=NOW),
            ],
        )
    )

    return sched


def test_sched_add_pop():
    sched = Scheduler(pool_size=1)

    job1, job2 = Job(fn=lambda x: x, args=[1]), Job(fn=lambda x: x, args=[2])
    sched.push(job1)
    sched.push(job2)

    with pytest.raises(SchedulerError):
        sched.push("not a job")

    assert len(sched) == 2

    assert sched.pop() is job1
    assert len(sched) == 1
    assert sched.pop() is job2
    assert not len(sched)

    with pytest.raises(SchedulerError):
        sched.pop()


def test_sched_add_pop_with_runs():
    sched = Scheduler(pool_size=2)

    job1, job2 = Job(fn=_fn), Job(fn=_fn, args=[1, 2])
    sched.push(job1)
    sched.push(job2)

    assert len(sched) == 2

    res = sched.run()
    assert len(sched) == 0
    assert res == [0, 2]

    job3 = Job(fn=_fn, args=[1, 3], kwargs={"2": 4})
    sched.push(job3)
    assert len(sched) == 1

    res = sched.run()
    assert not len(sched)
    assert res == [3]
