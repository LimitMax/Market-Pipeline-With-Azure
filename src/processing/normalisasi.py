from typing import Dict
from datetime import date
import pandas as pd

from common.errors import DataValidationError


def normalize_to_hourly(
    cleaned_data: Dict[str, pd.DataFrame],
    execution_date: date,
) -> Dict[str, pd.DataFrame]:

    hourly_data: Dict[str, pd.DataFrame] = {}

    for asset, df in cleaned_data.items():
        if df.empty:
            raise DataValidationError(
                f"Cleaned data empty for asset={asset} "
                f"on execution_date={execution_date}"
            )

        hourly_df = _normalize_single_asset(df, asset, execution_date)
        hourly_data[asset] = hourly_df

    return hourly_data


def _normalize_single_asset(
    df: pd.DataFrame,
    asset: str,
    execution_date: date,
) -> pd.DataFrame:
    # Ensure timestamp index
    df = df.set_index("timestamp").sort_index()

    # Build full hourly index for the execution date (UTC)
    start_ts = pd.Timestamp(execution_date, tz="UTC")
    end_ts = start_ts + pd.Timedelta(hours=23)

    full_index = pd.date_range(
        start=start_ts,
        end=end_ts,
        freq="h",
        tz="UTC",
    )

    # Resample to hourly (last known within the hour)
    hourly = df.resample("h").agg({
        "open_price": "first",
        "high_price": "max",
        "low_price": "min",
        "close_price": "last",
        "volume": "sum",
    })

    # Reindex to full 24-hour grid
    hourly = hourly.reindex(full_index)

    # Track gaps BEFORE filling
    data_gap_flag = hourly["close_price"].isna()

    # Fill missing prices using forward-fill
    price_cols = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
    ]
    hourly[price_cols] = hourly[price_cols].ffill()

    # Fill missing volume with 0
    hourly["volume"] = hourly["volume"].fillna(0)

    # Final validation (no NaN allowed)
    if hourly[price_cols].isna().any().any():
        raise DataValidationError(
            f"Unfillable price gap detected for asset={asset} "
            f"on execution_date={execution_date}"
        )

    # Add metadata columns
    hourly = hourly.reset_index().rename(columns={"index": "timestamp"})
    hourly["hour_key"] = hourly["timestamp"].dt.strftime("%Y%m%d%H")
    hourly["data_gap_flag"] = data_gap_flag.values
    hourly["asset"] = asset

    # Enforce final ordering
    hourly = hourly[
        [
            "asset",
            "hour_key",
            "timestamp",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "data_gap_flag",
        ]
    ]

    return hourly
