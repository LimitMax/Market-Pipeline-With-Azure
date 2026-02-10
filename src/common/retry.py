import time
import logging

from typing import Callable, Type

logger = logging.getLogger(__name__)

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

            logger.warning(
                "Retrying operation",
                extra={
                    "event": "RETRY",
                    "attempt": attempt,
                    "max_retries": retries,
                    "error": str(err),
                },
            )
