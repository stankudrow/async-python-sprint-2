from datetime import datetime, timedelta
from typing import Any

from freezegun import freeze_time
import pytest

from sprint2.job import Job


SECOND = 1
NOW = datetime.now()
FUTURE = NOW + timedelta(seconds=SECOND)
PAST = NOW - timedelta(seconds=SECOND)


def _fn():
    return None


class _Functor:
    def __call__(self, *args, **kwargs):
        return len(args) + len(kwargs)


# USE DURATION
@pytest.mark.parametrize(
    ("job1", "job2", "answer"),
    [
        (
            Job(fn=_fn, start=None),
            Job(fn=_fn, start=None),
            True,
        ),
        (
            Job(fn=_fn, start=None),
            Job(fn=_fn, start=NOW),
            True,
        ),
        (
            Job(fn=_fn, start=NOW),
            Job(fn=_fn, start=None),
            False,
        ),
        (
            Job(fn=_fn, start=NOW),
            Job(fn=_fn, start=NOW),
            False,
        ),
        (
            Job(fn=_fn, start=PAST),
            Job(fn=_fn, start=NOW),
            True,
        ),
        (
            Job(fn=_fn, start=FUTURE),
            Job(fn=_fn, start=NOW),
            False,
        ),
        (
            Job(fn=_fn, start=None, duration=None),
            Job(fn=_fn, start=None, duration=1),
            False,
        ),
        (
            Job(fn=_fn, start=NOW, duration=None),
            Job(fn=_fn, start=NOW, duration=1),
            False,
        ),
    ],
)
def test_job_less_than_operator(job1: Job, job2: Job, answer: bool):
    res = job1 < job2

    assert res == answer


@pytest.mark.parametrize(
    ("job1", "job2", "answer"),
    [
        (
            Job(fn=_fn),
            Job(fn=_fn),
            True,
        ),
        (
            Job(fn=_fn, start=None),
            Job(fn=_fn, start=NOW),
            False,
        ),
        (
            Job(fn=_fn, start=NOW),
            Job(fn=_fn, start=None),
            False,
        ),
        (
            Job(fn=_fn, start=NOW),
            Job(fn=_fn, start=NOW),
            True,
        ),
        (
            Job(fn=_fn, start=PAST),
            Job(fn=_fn, start=NOW),
            False,
        ),
        (
            Job(fn=_fn, start=FUTURE),
            Job(fn=_fn, start=NOW),
            False,
        ),
    ],
)
def test_job_equals_operator(job1: Job, job2: Job, answer: bool):
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
                "max_retries": None,
                "start": None,
                "duration": None,
                "dependencies": [],
            },
        ),
        (
            Job(
                fn=_fn,
                args=[1, "2"],
                kwargs={"a": 7, "b": [4]},
                max_retries=0,
                start=PAST,
                duration=1,
                dependencies=[Job(fn=_Functor)],
            ),
            {
                "fn": _fn,
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
                        "max_retries": None,
                        "start": None,
                        "duration": None,
                        "dependencies": [],
                    },
                ],
            },
        ),
        (
            Job(
                fn=_fn,
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
                            Job(fn=_fn, start=None, duration=1),
                            Job(fn=_Functor, start=None, duration=0),
                        ],
                    ),
                    Job(fn=_fn, start=NOW),
                ],
            ),
            {
                "fn": _fn,
                "args": (1, "2"),
                "kwargs": {"a": 7, "b": [4]},
                "max_retries": 0,
                "start": PAST,
                "duration": 1,
                "dependencies": [
                    {
                        "fn": _fn,
                        "args": (),
                        "kwargs": {},
                        "max_retries": None,
                        "start": NOW,
                        "duration": None,
                        "dependencies": [],
                    },
                    {
                        "fn": _Functor,
                        "args": (),
                        "kwargs": {},
                        "max_retries": None,
                        "start": FUTURE,
                        "duration": 0,
                        "dependencies": [
                            {
                                "fn": _Functor,
                                "args": (),
                                "kwargs": {},
                                "max_retries": None,
                                "start": None,
                                "duration": 0,
                                "dependencies": [],
                            },
                            {
                                "fn": _fn,
                                "args": (),
                                "kwargs": {},
                                "max_retries": None,
                                "start": None,
                                "duration": 1,
                                "dependencies": [],
                            },
                        ],
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


def test_job_iter():
    job_dep1 = Job(fn=_fn, start=NOW, duration=None)
    job_dep2 = Job(fn=_fn, start=PAST, duration=None)
    job_dep3 = Job(fn=_fn, start=FUTURE, duration=None)

    job_dep4 = Job(fn=_fn, start=None, duration=1, dependencies=[job_dep1, job_dep2])
    job_dep5 = Job(fn=_fn, start=None, duration=0, dependencies=[job_dep3])

    job = Job(
        fn=_Functor,
        start=FUTURE,
        duration=None,
        dependencies=[
            job_dep4,
            job_dep5,
        ],
    )

    answer = [job_dep3, job_dep5, job_dep2, job_dep1, job_dep4, job]

    assert list(job) == answer


@pytest.mark.parametrize(
    ("job", "deadline"),
    [
        (
            Job(fn=_fn, start=None, duration=None),
            None,
        ),
        (
            Job(fn=_fn, start=PAST, duration=None),
            None,
        ),
        (
            Job(fn=_fn, start=PAST, duration=0),
            PAST,
        ),
        (
            Job(fn=_fn, start=PAST, duration=SECOND),
            NOW,
        ),
        (
            Job(fn=_fn, start=None, duration=SECOND),
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
            Job(fn=_fn),
            True,
        ),
        (
            Job(fn=_fn, start=PAST),
            True,
        ),
        (
            Job(fn=_fn, start=NOW),
            True,
        ),
        (
            Job(fn=_fn, start=FUTURE),
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
            Job(fn=_fn),
            False,
        ),
        (
            Job(fn=_fn, start=PAST),
            False,
        ),
        (
            Job(fn=_fn, start=PAST, duration=0),
            True,
        ),
        (
            Job(fn=_fn, start=PAST, duration=SECOND),
            False,
        ),
    ],
)
def test_is_expired(job: Job, answer: bool):
    with freeze_time(NOW):
        assert job.is_expired() == answer
