from datetime import datetime
from typing import Generator

from sprint2.aiotools import coroutine, async_wait, async_sleep
from sprint2.jobtools.job import Job, JobError, validate_job_type
from sprint2.logger import sched_logger


__all__ = ["async_run_job"]


def _check_job_expired(job: Job) -> None:
    if job.is_expired():
        msg = f"the {job} is expired"
        raise TimeoutError(msg)


# these yield expressions are like async await statements
@coroutine
def async_run_job(job: Job) -> Generator:
    validate_job_type(job)
    yield from async_wait(*[async_run_job(dep) for dep in job.dependencies])
    _check_job_expired(job)
    if (start := job.start) and (not job.is_startable()):
        to_sleep: float = (start - datetime.now()).seconds
        sched_logger.info(f"{job}: sleeping for {to_sleep} seconds")
        for _ in async_sleep(seconds=to_sleep):
            yield
            _check_job_expired(job)
    f = job.run
    result = None
    yield
    if max_retries := job.max_retries:
        sched_logger.info(f"{job}: trying {max_retries} times")
        retries = 1
        while retries < max_retries:
            _check_job_expired(job)
            try:
                result = f()
                break
            except Exception:
                sched_logger.warning(f"{job}: the retry {retries} failed")
                yield
    _check_job_expired(job)
    try:
        result = f()
    except Exception as e:
        msg = f"{job} has failed with an exception: {e}"
        sched_logger.exception(msg)
        raise JobError(str(e)) from e
    yield
    sched_logger.info(f"{job}: finished with the result {result!r}")
    return result
