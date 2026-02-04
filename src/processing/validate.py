from typing import Dict, List
from datetime import date

from common.errors import DataValidationError

def validate_raw_data(
        raw_data: Dict[str, object],
        expected_assets: List[str],
        execution_date: date
) -> None:
    if not raw_data:
        raise DataValidationError(f"Raw data is empty for execution_date={execution_date}")
    
    missing_assets = [
        asset for asset in expected_assets if asset not in raw_data
    ]

    if missing_assets:
        raise DataValidationError(
            f"Missing assets in raw data: {missing_assets} "
            f"for execution_date={execution_date}"
        )
    
    for asset, data in raw_data.items():
        if data is None:
            raise DataValidationError(
                f"Raw data for asset={asset} is None "
                f"on execution_date={execution_date}"
            )
        
def validate_hourly_data(
        hourly_data: Dict[str,object],
        execution_date: date,
) -> None:
    if not hourly_data:
        raise DataValidationError(
            f"Hourly data is empty for execution_date={execution_date}"
        )

    for asset, df in hourly_data.items():
        if df.empty:
            raise DataValidationError(
                f"Hourly data empty for asset={asset} "
                f"on execution_date={execution_date}"
            )

        _validate_no_duplicate_hour(df, asset, execution_date)
        _validate_no_missing_hour(df, asset, execution_date)
        _validate_price_and_volume(df, asset, execution_date)

def _validate_no_duplicate_hour(df, asset: str, execution_date: date) -> None:
    duplicated = df["hour_key"].duplicated().any()
    if duplicated:
        raise DataValidationError(
            f"Duplicate hour_key detected for asset={asset} "
            f"on execution_date={execution_date}"
        )


def _validate_no_missing_hour(df, asset: str, execution_date: date) -> None:
    expected_hours = 24
    actual_hours = df["hour_key"].nunique()

    if actual_hours != expected_hours:
        raise DataValidationError(
            f"Missing hour detected for asset={asset}. "
            f"Expected {expected_hours} hours, got {actual_hours} "
            f"on execution_date={execution_date}"
        )


def _validate_price_and_volume(df, asset: str, execution_date: date) -> None:
    price_columns = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
    ]

    for col in price_columns:
        if (df[col] <= 0).any():
            raise DataValidationError(
                f"Invalid price detected in column={col} "
                f"for asset={asset} on execution_date={execution_date}"
            )

    if (df["volume"] < 0).any():
        raise DataValidationError(
            f"Negative volume detected for asset={asset} "
            f"on execution_date={execution_date}"
        )