from datetime import date
from typing import List

from common.logging import (
    log_pipeline_start,
    log_pipeline_end,
    log_error,
)
from common.config import load_active_assets
from common.errors import (
    SourceError,
    DataValidationError,
)

from ingestion.yfinance import extract_market_data
from processing.clean import clean_market_data
from processing.normalisasi import normalize_to_hourly
from processing.validate import (
    validate_raw_data,
    validate_hourly_data,
)
from storage.market_repository import write_fact_market_hourly
from common.pipeline_run import (
    generate_run_id,
    start_pipeline_run,
    complete_pipeline_run,
)
from common.retry import retry


PIPELINE_NAME = "market_pipeline"


def run_market_pipeline(
    run_type: str,
    execution_date: date,
) -> None:
    """
    Orchestrates end-to-end market data pipeline.

    run_type: 'scheduled' | 'backfill'
    execution_date: logical date being processed (UTC)
    """

    pipeline_run_id = generate_run_id()

    start_pipeline_run(
        pipeline_run_id=pipeline_run_id,
        pipeline_name=PIPELINE_NAME,
        run_type=run_type,
        execution_date=execution_date,
    )

    log_pipeline_start(
        pipeline_name=PIPELINE_NAME,
        run_id=pipeline_run_id,
        execution_date=execution_date,
    )

    try:
        # 1. Load asset scope
        assets: List[str] = load_active_assets()
        if not assets:
            raise DataValidationError("Asset list is empty")

        # 2. Extract raw data (with retry for source failure)
        raw_data = retry(
            func=extract_market_data,
            retries=3,
            retry_on=SourceError,
            assets=assets,
            execution_date=execution_date,
            pipeline_run_id=pipeline_run_id,
        )

        # 3. Validate raw ingestion
        validate_raw_data(
            raw_data=raw_data,
            expected_assets=assets,
            execution_date=execution_date,
        )

        # 4. Clean & standardize
        cleaned_data = clean_market_data(raw_data)

        # 5. Normalize to hourly granularity
        hourly_data = normalize_to_hourly(
            cleaned_data=cleaned_data,
            execution_date=execution_date,
        )

        # 6. Validate analytics contract
        validate_hourly_data(
            hourly_data=hourly_data,
            execution_date=execution_date,
        )

        # 7. Load analytics-ready fact table (idempotent)
        write_fact_market_hourly(
            hourly_data=hourly_data,
            pipeline_run_id=pipeline_run_id,
        )

        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            status="SUCCESS",
        )

    except SourceError as err:
        log_error(
            pipeline_run_id=pipeline_run_id,
            step="EXTRACT",
            error_type="SOURCE_ERROR",
            error=err,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            status="PARTIAL_SUCCESS",
        )

    except DataValidationError as err:
        log_error(
            pipeline_run_id=pipeline_run_id,
            step="VALIDATION",
            error_type="DATA_ERROR",
            error=err,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            status="FAILED",
        )

    except Exception as err:
        log_error(
            pipeline_run_id=pipeline_run_id,
            step="SYSTEM",
            error_type="SYSTEM_ERROR",
            error=err,
        )
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            status="FAILED",
        )

    finally:
        log_pipeline_end(
            pipeline_name=PIPELINE_NAME,
            run_id=pipeline_run_id,
        )
