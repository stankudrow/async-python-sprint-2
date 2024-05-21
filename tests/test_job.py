from datetime import datetime, timedelta
from typing import Any

from freezegun import freeze_time
import pytest

from sprint2.job import Job
from sprint2.aiotools import wait, async_sleep


SECOND = 1
NOW = datetime.now()
FUTURE = NOW + timedelta(seconds=SECOND)
PAST = NOW - timedelta(seconds=SECOND)


def _foo():
    return None


class _Functor:
    def __call__(self, *args, **kwargs):
        return len(args) + len(kwargs)


def _goo():
    wait(async_sleep(0.03))
    print("_goo")
    return 1


def _hoo():
    wait(async_sleep(0.02))
    print("_hoo")
    return 2


def _roo():
    print("_roo")
    wait(async_sleep(0.01))
    return 3


@pytest.mark.parametrize(
    ("job1", "job2", "answer"),
    [
        (
            Job(fn=_foo),
            Job(fn=_foo),
            True,
        ),
        (
            Job(fn=_foo, start=None),
            Job(fn=_foo, start=NOW),
            False,
        ),
        (
            Job(fn=_foo, start=NOW),
            Job(fn=_foo, start=None),
            False,
        ),
        (
            Job(fn=_foo, start=NOW),
            Job(fn=_foo, start=NOW),
            True,
        ),
        (
            Job(fn=_foo, start=PAST),
            Job(fn=_foo, start=NOW),
            False,
        ),
        (
            Job(fn=_foo, start=FUTURE),
            Job(fn=_foo, start=NOW),
            False,
        ),
    ],
)
def test_job_equality_operator(job1: Job, job2: Job, answer: bool):
    res = job1 == job2

    assert res == answer


@pytest.mark.parametrize(
    ("job", "answer"),
    [
        (
            Job(fn=_Functor),
            {
                "fn": _Functor,
                "args": (),
                "kwargs": {},
                "max_retries": 0,
                "start": None,
                "duration": None,
                "dependencies": [],
            },
        ),
        (
            Job(
                fn=_foo,
                args=[1, "2"],
                kwargs={"a": 7, "b": [4]},
                max_retries=0,
                start=PAST,
                duration=1,
                dependencies=[Job(fn=_Functor)],
            ),
            {
                "fn": _foo,
                "args": (1, "2"),
                "kwargs": {"a": 7, "b": [4]},
                "max_retries": 0,
                "start": PAST,
                "duration": 1,
                "dependencies": [
                    {
                        "fn": _Functor,
                        "args": (),
                        "kwargs": {},
                        "max_retries": 0,
                        "start": None,
                        "duration": None,
                        "dependencies": [],
                    },
                ],
            },
        ),
        (
            Job(
                fn=_foo,
                args=[1, "2"],
                kwargs={"a": 7, "b": [4]},
                max_retries=0,
                start=PAST,
                duration=1,
                dependencies=[
                    Job(
                        fn=_Functor,
                        start=FUTURE,
                        duration=0,
                        dependencies=[
                            Job(fn=_foo, start=None, duration=1),
                            Job(fn=_Functor, start=None, duration=0),
                        ],
                    ),
                    Job(fn=_foo, start=NOW),
                ],
            ),
            {
                "fn": _foo,
                "args": (1, "2"),
                "kwargs": {"a": 7, "b": [4]},
                "max_retries": 0,
                "start": PAST,
                "duration": 1,
                "dependencies": [
                    {
                        "fn": _Functor,
                        "args": (),
                        "kwargs": {},
                        "max_retries": 0,
                        "start": FUTURE,
                        "duration": 0,
                        "dependencies": [
                            {
                                "fn": _foo,
                                "args": (),
                                "kwargs": {},
                                "max_retries": 0,
                                "start": None,
                                "duration": 1,
                                "dependencies": [],
                            },
                            {
                                "fn": _Functor,
                                "args": (),
                                "kwargs": {},
                                "max_retries": 0,
                                "start": None,
                                "duration": 0,
                                "dependencies": [],
                            },
                        ],
                    },
                    {
                        "fn": _foo,
                        "args": (),
                        "kwargs": {},
                        "max_retries": 0,
                        "start": NOW,
                        "duration": None,
                        "dependencies": [],
                    },
                ],
            },
        ),
    ],
)
def test_job_dict(job: Job, answer: dict[str, Any]) -> None:
    dct = job.to_dict()
    new_job = Job.from_dict(dct)

    assert dct == answer
    assert job == new_job


@pytest.mark.parametrize(
    ("job", "deadline"),
    [
        (
            Job(fn=_foo, start=None, duration=None),
            None,
        ),
        (
            Job(fn=_foo, start=PAST, duration=None),
            None,
        ),
        (
            Job(fn=_foo, start=PAST, duration=0),
            PAST,
        ),
        (
            Job(fn=_foo, start=PAST, duration=SECOND),
            NOW,
        ),
        (
            Job(fn=_foo, start=None, duration=SECOND),
            FUTURE,
        ),
    ],
)
def test_get_deadline(job: Job, deadline: datetime):
    with freeze_time(NOW):
        ddl = job.get_deadline()
        if ddl is None:
            assert ddl is deadline
        else:
            assert ddl == deadline


@pytest.mark.parametrize(
    ("job", "answer"),
    [
        (
            Job(fn=_foo),
            True,
        ),
        (
            Job(fn=_foo, start=PAST),
            True,
        ),
        (
            Job(fn=_foo, start=NOW),
            True,
        ),
        (
            Job(fn=_foo, start=FUTURE),
            False,
        ),
    ],
)
def test_is_startable(job: Job, answer: bool):
    with freeze_time(NOW):
        assert job.is_startable() == answer


@pytest.mark.parametrize(
    ("job", "answer"),
    [
        (
            Job(fn=_foo),
            False,
        ),
        (
            Job(fn=_foo, start=PAST),
            False,
        ),
        (
            Job(fn=_foo, start=PAST, duration=0),
            True,
        ),
        (
            Job(fn=_foo, start=PAST, duration=SECOND),
            False,
        ),
    ],
)
def test_is_expired(job: Job, answer: bool):
    with freeze_time(NOW):
        assert job.is_expired() == answer


# def test_job_gen_dependencies():
#     job_dep1 = Job(fn=_foo, start=NOW, duration=None)
#     job_dep2 = Job(fn=_foo, start=PAST, duration=None)
#     job_dep3 = Job(fn=_foo, start=FUTURE, duration=None)

#     job_dep4 = Job(fn=_foo, start=None, duration=1, dependencies=[job_dep1, job_dep2])
#     job_dep5 = Job(fn=_foo, start=None, duration=0, dependencies=[job_dep3])

#     job = Job(
#         fn=_Functor,
#         start=FUTURE,
#         duration=None,
#         dependencies=[
#             job_dep4,
#             job_dep5,
#         ],
#     )

#     answer = [job_dep3, job_dep5, job_dep2, job_dep1, job_dep4]

#     assert list(job.gen_dependencies()) == answer


# def test_run_job_coroutine():
#     jobs = [
#         Job(
#             fn=_goo,
#         ),
#         Job(
#             fn=_hoo
#         ),
#         Job(
#             fn=_roo
#         )
#     ]

#     res = wait(*[run_job(job) for job in jobs])

#     # assert res == [3, 2, 1]
#     print(res)
