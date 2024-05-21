from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, Mapping

from pydantic import BaseModel, NonNegativeInt, ValidationError


__all__ = [
    "Job",
    "JobError",
    "JobInfo",
]


class JobError(Exception):
    pass


class JobInfo(BaseModel):
    fn: Callable
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = {}
    max_retries: NonNegativeInt = 0
    start: None | datetime = None
    duration: None | NonNegativeInt = None


def _validate_job(job: "Job") -> "Job":
    if not isinstance(job, Job):
        msg = f"{job} is not Job"
        raise JobError(msg)
    return job


class Job:
    def __init__(
        self,
        fn: Callable,
        args: None | Iterable[Any] = None,
        kwargs: None | Mapping[str, Any] = None,
        max_retries: NonNegativeInt = 0,
        start: None | datetime = None,
        duration: None | NonNegativeInt = None,
        dependencies: None | Iterable["Job"] = None,
    ) -> None:
        try:
            self._info = JobInfo(
                fn=fn,
                args=tuple(args) if args else (),
                kwargs=dict(kwargs) if kwargs else {},
                max_retries=max_retries,
                start=start,
                duration=duration,
            )
        except ValidationError as e:
            raise JobError(str(e)) from e
        self._deps: list["Job"] = (
            [_validate_job(job) for job in dependencies] if dependencies else []
        )
        self._runner = JobRunnerStrategy(self)

    def __eq__(self, other) -> bool:
        sinfo = self._info
        if isinstance(other, Job):
            info = sinfo == other._info
            deps = self.dependencies == other.dependencies
            return info and deps
        return sinfo == other

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        dct = self.to_dict()
        return f"{cls_name}<{dct}>"

    @property
    def func(self) -> Callable:
        return self._info.fn

    @property
    def args(self) -> tuple[Any, ...]:
        return self._info.args

    @property
    def kwargs(self) -> dict[str, Any]:
        return self._info.kwargs

    @property
    def max_retries(self) -> None | NonNegativeInt:
        return self._info.max_retries

    @property
    def start(self) -> None | datetime:
        return self._info.start

    @property
    def duration(self) -> None | NonNegativeInt:
        return self._info.duration

    @property
    def dependencies(self) -> list["Job"]:
        return self._deps

    # def gen_dependencies(self) -> Generator["Job", None, None]:
    #     yield from self._runner.gen_dependencies()

    def get_deadline(self) -> None | datetime:
        now_ = datetime.now()
        start = self.start
        if start is None:
            start = now_
        duration = self.duration
        if duration is not None:
            return start + timedelta(seconds=duration)
        return None

    def is_expired(self) -> bool:
        deadline = self.get_deadline()
        if deadline is None:
            return False
        now_ = datetime.now()
        return now_ > deadline

    def is_startable(self) -> bool:
        start = self.start
        if start is None:
            return True
        now_ = datetime.now()
        return now_ >= start

    @classmethod
    def from_dict(cls, dct: dict[str, Any]) -> "Job":
        """Return a job instance from the dictionary."""

        params = deepcopy(dct)
        deps = []
        for dependant in params.pop("dependencies"):
            deps.append(Job.from_dict(dependant))
        return Job(**params, dependencies=deps)

    def to_dict(self) -> dict[str, Any]:
        """Return the job as a dictionary."""

        return {
            "fn": self.func,
            "args": self.args,
            "kwargs": self.kwargs,
            "start": self.start,
            "max_retries": self.max_retries,
            "duration": self.duration,
            "dependencies": [dep_job.to_dict() for dep_job in self._deps],
        }

    # def run(self):
    #     yield from self._runner.step()


class JobRunnerStrategy:
    def __init__(self, job: "Job"):
        self._job = job

    def gen_dependencies(self):
        for job in self._job.dependencies:
            if job.dependencies:
                yield from job.gen_dependencies()
            yield job

    # def step(self) -> Generator[Any, None, Any]:
    #     yield from async_wait(run_job(self._job))
