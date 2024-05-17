from datetime import datetime
from typing import Any, Callable

from pydantic import BaseModel, NonNegativeInt


class Job(BaseModel):
    fn: Callable
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = {}
    max_retries: NonNegativeInt = 0
    start: None | datetime = None
    duration: None | NonNegativeInt = None
    dependencies: tuple["Job", ...] = ()

    def __lt__(self, other: "Job") -> bool:
        if self.start is None:
            return True
        if other.start is None:
            return False
        return self.start < other.start

    # redundant, but OK
    def to_dict(self) -> dict[str, Any]:
        """Returns the job as a dictionary.

        Returns:
            dict[str, Any]
        """

        return self.model_dump()
