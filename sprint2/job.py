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
    dependencies: tuple["JobInfo", ...] = ()


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
            deps = [dep_job.info for dep_job in dependencies] if dependencies else ()
            self._info = JobInfo(
                fn=fn,
                args=tuple(args) if args else (),
                kwargs=dict(kwargs) if kwargs else {},
                max_retries=max_retries,
                start=start,
                duration=duration,
                dependencies=tuple(deps),
            )
        except (AttributeError, ValidationError) as e:
            raise JobError(str(e)) from e

    def __eq__(self, other) -> bool:
        sinfo = self._info
        if isinstance(other, Job):
            return sinfo == other._info
        return sinfo == other

    def __lt__(self, other: "Job") -> bool:
        sst = self.info.start
        ost = other.info.start
        if sst is None:
            return True
        if ost is None:
            return False
        return sst < ost

    @property
    def info(self) -> JobInfo:
        """Return the information about the job.

        Returns:
            JobInfo - the deepcopy of the Job data
        """

        return copy.deepcopy(self._info)

    def to_dict(self) -> dict[str, typing.Any]:
        """Returns the job as a dictionary.

        Returns:
            dict[str, Any]
        """

        return self._info.model_dump()
