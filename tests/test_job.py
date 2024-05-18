from datetime import datetime, timedelta
from typing import Any

import pytest

from sprint2.job import Job


NOW = datetime.now()
FUTURE = NOW + timedelta(microseconds=1)
PAST = NOW - timedelta(microseconds=1)


def _fn():
    return None


class _Functor:
    def __call__(self, *args, **kwargs):
        return len(args) + len(kwargs)


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
    ],
)
def test_job_less_than(job1: Job, job2: Job, answer: bool) -> None:
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
def test_job_equality(job1: Job, job2: Job, answer: bool) -> None:
    res = job1 == job2

    assert res == answer


@pytest.mark.parametrize(
    ("job", "answer"),
    [
        (
            Job(fn=_fn),
            {
                "info": {
                    "fn": _fn,
                    "args": (),
                    "kwargs": {},
                    "max_retries": None,
                    "start": None,
                    "duration": None,
                },
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
                "info": {
                    "fn": _fn,
                    "args": (1, "2"),
                    "kwargs": {"a": 7, "b": [4]},
                    "max_retries": 0,
                    "start": PAST,
                    "duration": 1,
                },
                "dependencies": [
                    {
                        "info": {
                            "fn": _Functor,
                            "args": (),
                            "kwargs": {},
                            "max_retries": None,
                            "start": None,
                            "duration": None,
                        },
                        "dependencies": [],
                    },
                ],
            },
        ),
    ],
)
def test_job_to_dict(job: Job, answer: dict[str, Any]) -> None:
    dct = job.to_dict()

    assert dct == answer
