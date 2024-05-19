import copy
import datetime
import typing

from pydantic import BaseModel, NonNegativeInt, ValidationError


class JobError(Exception):
    pass


class JobInfo(BaseModel):
    fn: typing.Callable
    args: tuple[typing.Any, ...] = ()
    kwargs: dict[str, typing.Any] = {}
    max_retries: None | NonNegativeInt = None
    start: None | datetime.datetime = None
    duration: None | NonNegativeInt = None


def validate_job(job: "Job") -> "Job":
    if not isinstance(job, Job):
        msg = f"{job} is not Job"
        raise JobError(msg)
    return job


class Job:
    def __init__(
        self,
        fn: typing.Callable,
        args: None | typing.Iterable[typing.Any] = None,
        kwargs: None | typing.Mapping[str, typing.Any] = None,
        max_retries: None | NonNegativeInt = None,
        start: None | datetime.datetime = None,
        duration: None | NonNegativeInt = None,
        dependencies: None | typing.Iterable["Job"] = None,
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
            self._dependencies: list["Job"] = (
                sorted(validate_job(job) for job in dependencies)
                if dependencies
                else []
            )
        except (AttributeError, ValidationError) as e:
            raise JobError(str(e)) from e

    def __eq__(self, other) -> bool:
        sinfo = self._info
        if isinstance(other, Job):
            return sinfo == other._info
        return sinfo == other

    def __iter__(self) -> typing.Generator["Job", None, None]:
        for dependant in self.dependencies:
            yield from dependant
        yield self

    def __lt__(self, other: "Job") -> bool:
        sst, ost = self.start, other.start
        if sst and ost:
            return sst < ost
        if sst and ost is None:
            return False
        if ost and sst is None:
            return True
        sdur, odur = self.duration, other.duration
        if sdur and odur:
            return sdur < odur
        if sdur and odur is None:
            return True
        if odur and sdur is None:
            return False
        return True

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        dct = self.to_dict()
        return f"{cls_name}<{dct}>"

    @property
    def func(self) -> typing.Callable:
        return self._info.fn

    @property
    def args(self) -> tuple[typing.Any, ...]:
        return self._info.args

    @property
    def kwargs(self) -> dict[str, typing.Any]:
        return self._info.kwargs

    @property
    def retries(self) -> None | NonNegativeInt:
        return self._info.max_retries

    @property
    def start(self) -> None | datetime.datetime:
        return self._info.start

    @property
    def duration(self) -> None | NonNegativeInt:
        return self._info.duration

    @property
    def dependencies(self) -> list["Job"]:
        """Return the dependencies of the job."""

        return self._dependencies

    def get_deadline(self) -> None | datetime.datetime:
        now_ = datetime.datetime.now()
        start = self.start
        if start is None:
            start = now_
        duration = self.duration
        if duration is not None:
            return start + datetime.timedelta(seconds=duration)
        return None

    def is_expired(self) -> bool:
        deadline = self.get_deadline()
        if deadline is None:
            return False
        now_ = datetime.datetime.now()
        return now_ > deadline

    def is_startable(self) -> bool:
        start = self.start
        if start is None:
            return True
        now_ = datetime.datetime.now()
        return now_ >= start

    @classmethod
    def from_dict(cls, dct: dict[str, typing.Any]) -> "Job":
        params = copy.deepcopy(dct)
        deps = []
        for dependant in params.pop("dependencies"):
            deps.append(Job.from_dict(dependant))
        return Job(**params, dependencies=deps)

    def to_dict(self) -> dict[str, typing.Any]:
        """Return the job as a dictionary."""

        attrs = {
            "fn": self.func,
            "args": self.args,
            "kwargs": self.kwargs,
            "start": self.start,
            "max_retries": self.retries,
            "duration": self.duration,
            "dependencies": [dep_job.to_dict() for dep_job in self._dependencies],
        }
        return attrs
