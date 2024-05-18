from datetime import datetime
from typing import Any, Callable, Iterable, Mapping

from pydantic import BaseModel, NonNegativeInt, ValidationError


class JobError(Exception):
    pass


class JobInfo(BaseModel):
    fn: Callable
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = {}
    max_retries: None | NonNegativeInt = None
    start: None | datetime = None
    duration: None | NonNegativeInt = None
    dependencies: tuple["JobInfo", ...] = ()


class Job:
    def __init__(
        self,
        fn: Callable,
        args: None | Iterable[Any] = None,
        kwargs: None | Mapping[str, Any] = None,
        max_retries: None | NonNegativeInt = None,
        start: None | datetime = None,
        duration: None | NonNegativeInt = None,
        dependencies: None | Iterable["Job"] = None,
    ) -> None:
        try:
            deps = [dep_job.info for dep_job in dependencies] if dependencies else ()
            self._info = JobInfo(
                fn=fn,
                args=args if args else (),
                kwargs=kwargs if kwargs else {},
                max_retries=max_retries,
                start=start,
                duration=duration,
                dependencies=deps,
            )
        except (AttributeError, ValidationError) as e:
            raise JobError(str(e)) from e

    def __eq__(self, other: "Job") -> bool:
        return self._info == other._info

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

        return self._info

    def to_dict(self) -> dict[str, Any]:
        """Returns the job as a dictionary.

        Returns:
            dict[str, Any]
        """

        return self._info.model_dump()
