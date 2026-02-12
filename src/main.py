import os
from datetime import date, datetime, timedelta
from pipeline.market_pipeline import run_market_pipeline
from common.logging_config import setup_logging
from pipeline.scheduler_utils import determine_execution_dates

setup_logging()

def parse_execution_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("EXECUTION_DATE must be YYYY-MM-DD")

if __name__ == "__main__":
    print("Running with ENV =", os.getenv("ENV"))
    print("STORAGE_BASE_PATH =", os.getenv("STORAGE_BASE_PATH"))

    manual_date = os.getenv("EXECUTION_DATE")

    if manual_date:
        # ================================
        # 🔐 OPTIONAL SAFETY GUARD (DI SINI)
        # ================================
        if os.getenv("ENV") == "cloud":
            raise RuntimeError(
                "Manual EXECUTION_DATE is not allowed in cloud environment"
            )

        execution_date = parse_execution_date(manual_date)

        print(f"[MANUAL MODE] Running pipeline for {execution_date}")

        run_market_pipeline(
            run_type="manual",
            execution_date=execution_date,
        )

    else:
        # ================================
        # 🤖 AUTO SCHEDULE MODE
        # ================================
        today =  date.today() - timedelta(days=1)

        for execution_date in determine_execution_dates(today):
            run_market_pipeline(
                run_type="scheduled",
                execution_date=execution_date,
            )