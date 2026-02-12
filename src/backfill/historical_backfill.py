import time
from uuid import uuid4
from datetime import date, timedelta, datetime

from common.errors import PipelineError
from common.logging_config import setup_logging
from common.progress_log import header, process, item, result

from pipeline.market_pipeline import run_market_pipeline
from storage.pipeline_event_repository import write_pipeline_event

setup_logging()

def daterange(start_date: date, end_date: date):
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
    Historical backfill runner.

    Design:
    - Reuses the SAME market pipeline
    - One backfill_id for observability
    - Per-date failure does NOT stop the backfill
    """

    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    backfill_id = str(uuid4())
    start_time = datetime.utcnow()

    # -------------------------------
    # BACKFILL START (PROGRESS)
    # -------------------------------
    header(
        title="MARKET PIPELINE BACKFILL",
        execution_date=f"{start_date} → {end_date}",
        run_type="backfill",
    )

    process("Starting historical backfill")

    # -------------------------------
    # BACKFILL START (OPS EVENT)
    # -------------------------------
    write_pipeline_event({
        "event_type": "BACKFILL_START",
        "backfill_id": backfill_id,
        "execution_date": start_date,
        "start_date": start_date,
        "end_date": end_date,
        "start_time": start_time,
    })

    success_days = 0
    failed_days = 0

    # -------------------------------
    # PER-DATE BACKFILL
    # -------------------------------
    for execution_date in daterange(start_date, end_date):
        process(f"Processing date {execution_date}")

        try:
            write_pipeline_event({
                "event_type": "BACKFILL_DATE_STARTED",
                "backfill_id": backfill_id,
                "execution_date": execution_date,
            })

            # CORE PIPELINE (UNCHANGED)
            run_market_pipeline(
                run_type="backfill",
                execution_date=execution_date,
            )

            write_pipeline_event({
                "event_type": "BACKFILL_DATE_SUCCEEDED",
                "backfill_id": backfill_id,
                "execution_date": execution_date,
            })

            item(str(execution_date), "SUCCESS")
            success_days += 1

            time.sleep(sleep_seconds)

        except PipelineError as err:
            write_pipeline_event({
                "event_type": "BACKFILL_DATE_FAILED",
                "backfill_id": backfill_id,
                "execution_date": execution_date,
                "error_message": str(err),
            })

            item(str(execution_date), "FAILED (pipeline error)")
            failed_days += 1
            continue

        except Exception as err:
            write_pipeline_event({
                "event_type": "BACKFILL_DATE_FAILED",
                "backfill_id": backfill_id,
                "execution_date": execution_date,
                "error_message": f"UNEXPECTED_ERROR: {err}",
            })

            item(str(execution_date), "FAILED (unexpected error)")
            failed_days += 1
            continue

    end_time = datetime.utcnow()

    # -------------------------------
    # BACKFILL END (OPS EVENT)
    # -------------------------------
    write_pipeline_event({
        "event_type": "BACKFILL_END",
        "backfill_id": backfill_id,
        "execution_date": end_date,
        "end_time": end_time,
        "duration_seconds": int((end_time - start_time).total_seconds()),
        "success_days": success_days,
        "failed_days": failed_days,
    })

    # -------------------------------
    # BACKFILL END (PROGRESS)
    # -------------------------------
    result(
        f"BACKFILL FINISHED | Success: {success_days} days | Failed: {failed_days} days"
    )


if __name__ == "__main__":
    # local test pake range kecil aja
    run_historical_backfill(
        start_date=date(2025, 1, 26),
        end_date=date.today(),
        sleep_seconds=2,
    )
