from datetime import date
from pipeline.market_pipeline import run_market_pipeline

if __name__ == "__main__":
    run_market_pipeline(
        run_type="scheduled",
        execution_date=date(2025, 2, 1),
    )
