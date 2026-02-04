import time
from typing import Callable, Type


def retry(
    func: Callable,
    retries: int,
    retry_on: Type[Exception],
    delay_seconds: int = 2,
    backoff_factor: int = 2,
    **kwargs,
):

    attempt = 0
    delay = delay_seconds

    while attempt <= retries:
        try:
            return func(**kwargs)

        except retry_on as err:
            attempt += 1

            if attempt > retries:
                raise

            time.sleep(delay)
            delay *= backoff_factor
