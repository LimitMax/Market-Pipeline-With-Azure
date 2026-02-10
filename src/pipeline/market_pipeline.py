from datetime import date

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
from common.pipeline_run import (
    generate_run_id,
    start_pipeline_run,
    complete_pipeline_run,
)
from common.retry import retry
from common.progress_log import header, process, item, result

from ingestion.yfinance import extract_market_data
from processing.clean import clean_market_data
from processing.normalisasi import normalize_to_hourly
from processing.validate import (
    validate_raw_data,
    validate_hourly_data,
)
from storage.market_repository import write_fact_market_hourly
from storage.pipeline_event_repository import write_pipeline_event
from storage.watermark_repository import update_daily_watermark


PIPELINE_NAME = "market_pipeline"


def run_market_pipeline(
    run_type: str,
    execution_date: date,
) -> None:
    """
    Market data pipeline.
    Same logic for scheduled & backfill runs.
    """

    pipeline_run_id = generate_run_id()

    # ==================================================
    # SYSTEM LOGS
    # ==================================================
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

    # ==================================================
    # HUMAN-READABLE PROGRESS LOG
    # ==================================================
    header(
        title="MARKET PIPELINE",
        execution_date=execution_date,
        run_type=run_type,
    )

    try:
        # ==================================================
        # 1. LOAD ASSETS
        # ==================================================
        assets = load_active_assets()
        if not assets:
            raise DataValidationError("Asset list is empty")

        asset_types = {a["symbol"]: a["type"] for a in assets}

        # ==================================================
        # 2. EXTRACT RAW DATA
        # ==================================================
        process("Fetching market data from yfinance", run_type=run_type)

        raw_data = {}

        is_weekend = execution_date.weekday() >= 5
        has_stock = any(a["type"] == "stock" for a in assets)

        # --- Market-level context ---
        if is_weekend and has_stock:
            item("STOCK MARKET", "CLOSED (weekend)", run_type=run_type)

        for asset in assets:
            symbol = asset["symbol"]
            asset_type = asset["type"]

            # Skip stocks on weekend
            if asset_type == "stock" and is_weekend:
                write_pipeline_event({
                    "pipeline_run_id": pipeline_run_id,
                    "pipeline_name": PIPELINE_NAME,
                    "event_type": "ASSET_SKIPPED",
                    "step": "EXTRACT",
                    "asset": symbol,
                    "asset_type": asset_type,
                    "reason": "NON_TRADING_DAY",
                    "execution_date": execution_date,
                })
                continue

            df = retry(
                func=extract_market_data,
                retries=3,
                retry_on=SourceError,
                symbol=symbol,
                asset_type=asset_type,
                execution_date=execution_date,
                pipeline_run_id=pipeline_run_id,
            )

            raw_data[symbol] = df
            hours = df["timestamp"].dt.hour.nunique()

            if asset_type == "crypto":
                item(symbol, f"SUCCESS ({hours} hours)", run_type=run_type)
            else:
                item(symbol, f"SUCCESS ({hours} hours, trading hours)", run_type=run_type)

        # ==================================================
        # 3. VALIDATE RAW INGESTION (ONCE)
        # ==================================================
        validate_raw_data(
            raw_data=raw_data,
            expected_assets=list(asset_types.keys()),
            asset_types=asset_types,
            execution_date=execution_date,
        )

        # ==================================================
        # 4. CLEAN DATA
        # ==================================================
        process("Cleaning raw market data", run_type=run_type)
        cleaned_data = clean_market_data(raw_data)

        # ==================================================
        # 5. NORMALIZE TO HOURLY (PER ASSET)
        # ==================================================
        process("Normalizing data to hourly", run_type=run_type)

        hourly_data = {}

        for asset, df in cleaned_data.items():
            asset_type = asset_types.get(asset)

            hourly = normalize_to_hourly(
                cleaned_data={asset: df},
                execution_date=execution_date,
                asset_type=asset_type,
            )

            hourly_df = hourly.get(asset)

            # Stock weekend / no trading data
            if hourly_df is None or hourly_df.empty:
                continue

            hourly_data[asset] = hourly_df

            if (
                "data_gap_flag" in hourly_df.columns
                and hourly_df["data_gap_flag"].any()
            ):
                item(asset, "OK (gap detected)", run_type=run_type)
            else:
                item(asset, "OK (no gap)", run_type=run_type)

        # ==================================================
        # 6. VALIDATE ANALYTICS CONTRACT
        # ==================================================
        validate_hourly_data(
            hourly_data=hourly_data,
            execution_date=execution_date,
            asset_types=asset_types,
        )

        # ==================================================
        # 7. WRITE TO DATA LAKE
        # ==================================================
        process("Writing data to data lake", run_type=run_type)

        written_assets = []

        for asset, df in hourly_data.items():
            try:
                write_fact_market_hourly(
                    hourly_data={asset: df},
                    pipeline_run_id=pipeline_run_id,
                )

                item(asset, "WRITTEN", run_type=run_type)
                written_assets.append(asset)

            except Exception as err:
                item(asset, f"FAILED TO WRITE ({err})", run_type=run_type)

                write_pipeline_event({
                    "pipeline_run_id": pipeline_run_id,
                    "pipeline_name": PIPELINE_NAME,
                    "event_type": "WRITE_FAILED",
                    "step": "WRITE",
                    "asset": asset,
                    "execution_date": execution_date,
                    "error_message": str(err),
                })

        


        # ==================================================
        # PIPELINE SUCCESS
        # ==================================================
        complete_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            status="SUCCESS",
        )

        result("PIPELINE SUCCESS", run_type=run_type)

        # ==================================================
        # 7b. UPDATE DAILY WATERMARK (SAFE)
        # ==================================================
        for asset in written_assets:
            update_daily_watermark(
                asset=asset,
                asset_type=asset_types[asset],
                last_date=execution_date,
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

        result("PIPELINE PARTIAL SUCCESS (source error)")

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

        result("PIPELINE FAILED (data validation error)")

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

        result("PIPELINE FAILED (system error)")

    finally:
        log_pipeline_end(
            pipeline_name=PIPELINE_NAME,
            run_id=pipeline_run_id,
        )
