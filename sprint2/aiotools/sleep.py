from datetime import datetime, timedelta
from typing import Generator


__all__ = ["async_sleep"]


def async_sleep(seconds: float) -> Generator[None, None, float]:
    deadline = datetime.now() + timedelta(seconds=seconds)
    while datetime.now() <= deadline:
        yield
    return seconds
