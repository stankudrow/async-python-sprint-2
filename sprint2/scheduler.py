"""The Scheduler Module."""

import heapq
import logging
import threading
from collections import deque

# from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy

from pydantic import BaseModel, NonNegativeInt, ValidationError, PositiveInt

from sprint2.job import Job


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class SchedulerError(Exception):
    """Generic Scheduler Error."""


def validate_job_type(job: Job) -> Job:
    if not isinstance(job, Job):
        msg = f"the {job} is not Job type"
        raise SchedulerError(msg)
    return job


# class _JobRunner(threading.Thread):
#     """The main background job runner thread."""

#     def __init__(
#         self,
#         pool_size: NonNegativeInt,
#     ) -> None:
#         self._executor = ThreadPoolExecutor(max_workers=pool_size)

#     def run(self):
#         with ThreadPoolExecutor()


class Scheduler:
    """A Simplified Scheduler for the Sprint 2."""

    class _SchedulerParams(BaseModel):
        pool_size: NonNegativeInt = 0

    def __init__(self, pool_size: int = 10):
        try:
            params = self._SchedulerParams(pool_size=pool_size)
            self._size = params.pool_size
        except ValidationError as e:
            raise SchedulerError(str(e))
        self._queue: list[Job] = []
        self._lock = threading.RLock()
        # self._pool = ThreadPoolExecutor(pool_size=pool_size)
        # self.run()

    def __getitem__(self, idx: int) -> Job:
        return self._queue[idx]

    def __len__(self) -> int:
        return len(self._queue)

    @property
    def pool_size(self) -> int:
        """Returns the pool_size as a non-negative integer."""
        return self._size

    @property
    def queue(self) -> list[Job]:
        """Returns the list of scheduled jobs."""
        with self._lock:
            qcopy = self._queue[:]
        return list(map(heapq.heappop, [qcopy] * len(qcopy)))

    def empty(self) -> bool:
        """Returns True if the scheduler is empty."""
        with self._lock:
            return not self._queue

    def add(self, job: Job) -> None:
        """Adds/Schedules the job."""
        job = validate_job_type(job)
        with self._lock:
            heapq.heappush(self._queue, deepcopy(job))

    def remove(self, job: Job) -> None:
        """Removes/Unschedules the job."""
        job = validate_job_type(job)
        with self._lock:
            self._queue.remove(job)
            heapq.heapify(self._queue)

    def pop(self) -> Job:
        with self._lock:
            return heapq.heappop(self._queue)

    def _is_schedulable(self) -> bool:
        return len(self._active_jobs) < self.pool_size

    def _run(self) -> None:
        # semaphore ??? limit according to the pool size
        # while self._state.is_running:
        pass

    def run(self) -> None:
        if not self.pool_size:
            LOGGER.info("the pool size is 0: no room for jobs")
            return
        self._state.next()
