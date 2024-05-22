from collections import deque
from enum import Enum
from typing import Generator
from threading import RLock

from pydantic import BaseModel, NonNegativeInt

from sprint2.aiotools import async_gather, gather, Coroutine
from sprint2.jobtools.job import Job, JobError, validate_job_type
from sprint2.jobtools.runners import async_run_job
from sprint2.logger import sched_logger


class SchedulerError(Exception):
    pass


class JobTaskStatus(str, Enum):
    CREATED = "CREATED"
    CANCELLED = "CANCELLED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"


class JobTask:
    def __init__(self, job: Job):
        validate_job_type(job)
        self.job = job
        self.coro: Coroutine = async_run_job(job)
        self.state = JobTaskStatus.CREATED


class Scheduler:
    """Actually it is a JobLoop."""

    class _SchedInfo(BaseModel):
        pool_size: NonNegativeInt

    def __init__(self, pool_size: NonNegativeInt = 10):
        self._psize: int = self._SchedInfo(pool_size=pool_size).pool_size
        self._jobs: deque[Job] = deque()
        self._tasks: deque[JobTask] = deque()
        self._lock = RLock()

    def __len__(self) -> int:
        return len(self._jobs)

    def pop(self) -> Job:
        with self._lock:
            try:
                job = self._jobs[0]
            except IndexError:
                msg = "pop a job from an empty scheduler"
                raise SchedulerError(msg)
            self._unschedule(job)
            return self._jobs.popleft()

    def _pop_task(self) -> None | JobTask:
        with self._lock:
            if len(self._tasks):
                return self._tasks.popleft()
            return None

    def push(self, job: Job) -> None:
        with self._lock:
            try:
                validate_job_type(job)
            except JobError as e:
                raise SchedulerError(str(e)) from e
            self._jobs.append(job)
            self._push_task(job)

    def _push_task(self, job: Job) -> None:
        with self._lock:
            if len(self._tasks) < min(len(self), self._psize):
                self._tasks.append(JobTask(job))

    def _unschedule(self, job: Job) -> None:
        task_to_unsched = None
        with self._lock:
            for task in self._tasks:
                if task.job is job:
                    task_to_unsched = task
                    break
            if t := task_to_unsched:
                if (s := t.state) != JobTaskStatus.CREATED:
                    msg = f"the {job} with status {s} is unschedulable"
                    sched_logger.exception(msg)
                    raise SchedulerError(msg)
                msg = f"the {job} is unscheduled"
                sched_logger.info(msg)
                self._tasks.remove(task_to_unsched)

    def _flush_taskified_jobs(self):
        with self._lock:
            while self._tasks:
                task = self._pop_task()
                self._jobs.remove(task.job)
                task.coro.close()

    def run(self) -> list:
        res: list[list] = gather(self.async_step())
        return res.pop()

    def async_step(self) -> Generator:
        runners = [task.coro for task in self._tasks]
        waiter = async_gather(*runners, return_exceptions=True)
        while True:
            try:
                yield next(waiter)
            except StopIteration as result:
                self._flush_taskified_jobs()
                return result.value
