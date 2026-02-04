from datetime import date, timedelta
import time

from pipeline.market_pipeline import run_market_pipeline
from common.logging import log_pipeline_start, log_pipeline_end
from common.errors import PipelineError


def daterange(start_date: date, end_date: date):
    """Yield dates from start_date to end_date (inclusive)."""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def run_historical_backfill(
    start_date: date,
    end_date: date,
    sleep_seconds: int = 2,
):
    """
    Backfill market data from start_date to end_date (UTC).

    - Reuses the same pipeline as scheduled runs
    - Idempotent per date
    - Safe to re-run
    """

    for execution_date in daterange(start_date, end_date):
        try:
            run_market_pipeline(
                run_type="backfill",
                execution_date=execution_date,
            )

            # Small delay to avoid rate limiting
            time.sleep(sleep_seconds)

        except PipelineError as err:
            # Do NOT stop entire backfill; continue with next date
            print({
                "event": "BACKFILL_DATE_FAILED",
                "execution_date": str(execution_date),
                "error": str(err),
            })
            continue


if __name__ == "__main__":
    # Example: backfill from Jan 1, 2025 to today
    run_historical_backfill(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 5),
        sleep_seconds=2,
    )
