from typing import Dict, List
from datetime import date

from common.errors import DataValidationError


def validate_raw_data(
    *,
    raw_data: Dict[str, object],
    expected_assets: List[str],
    asset_types: Dict[str, str],
    execution_date: date,
) -> None:
    """
    Validate raw ingestion results.
    Market-aware:
    - Stock weekend: missing is allowed
    - Others: missing is ERROR
    """

    if not raw_data:
        raise DataValidationError(
            f"No raw data ingested for execution_date={execution_date}"
        )

    for asset in expected_assets:
        if asset not in raw_data:
            asset_type = asset_types.get(asset)

            # Allow stock missing di weekend
            if asset_type == "stock" and execution_date.weekday() >= 5:
                continue

            raise DataValidationError(
                f"Missing raw data for asset={asset} "
                f"on execution_date={execution_date}"
            )

    for asset, data in raw_data.items():
        if data is None:
            raise DataValidationError(
                f"Raw data for asset={asset} is None "
                f"on execution_date={execution_date}"
            )

def validate_hourly_data(
    *,
    hourly_data: Dict[str, object],
    execution_date: date,
    asset_types: Dict[str, str],
) -> None:
    """
    Market-aware validation:
    - Crypto: must have exactly 24 hours
    - Stock weekday: >=1 hour is valid
    - Stock weekend: 0 hour is valid
    """

    if not hourly_data:
        return  # valid (e.g. all stocks on non-trading day)

    for asset, df in hourly_data.items():
        asset_type = asset_types.get(asset)

        if asset_type is None:
            raise DataValidationError(
                f"Unknown asset={asset} for execution_date={execution_date}"
            )

        if df.empty:
            if asset_type == "stock":
                continue  # market closed
            raise DataValidationError(
                f"{asset_type} asset={asset} has empty data "
                f"on execution_date={execution_date}"
            )

        # Market-specific hour checks
        actual_hours = df["hour_key"].nunique()

        if asset_type == "crypto":
            if actual_hours != 24:
                raise DataValidationError(
                    f"Crypto asset={asset} expected 24 hours, "
                    f"got {actual_hours} on execution_date={execution_date}"
                )

        elif asset_type == "stock":
            if actual_hours < 1:
                raise DataValidationError(
                    f"Stock asset={asset} has no intraday data "
                    f"on execution_date={execution_date}"
                )

        # Shared sanity checks
        _validate_no_duplicate_hour(df, asset, execution_date)
        _validate_price_and_volume(df, asset, execution_date)

# HELPER VALIDATIONS
def _validate_no_duplicate_hour(df, asset: str, execution_date: date) -> None:
    if df["hour_key"].duplicated().any():
        raise DataValidationError(
            f"Duplicate hour_key detected for asset={asset} "
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
