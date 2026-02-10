import pandas as pd

from typing import Dict
from datetime import date
from common.errors import DataValidationError

def normalize_to_hourly(
    cleaned_data: Dict[str, pd.DataFrame],
    execution_date: date,
    asset_type: str,
) -> Dict[str, pd.DataFrame]:

    hourly_data: Dict[str, pd.DataFrame] = {}

    for asset, df in cleaned_data.items():
        if df.empty:
            raise DataValidationError(
                f"Cleaned data empty for asset={asset} "
                f"on execution_date={execution_date}"
            )

        hourly_df = _normalize_single_asset(df, asset, execution_date, asset_type)
        hourly_data[asset] = hourly_df

    return hourly_data

def _normalize_single_asset(
    df: pd.DataFrame,
    asset: str,
    execution_date: date,
    asset_type: str,
) -> pd.DataFrame:

    if asset_type == "stock" and df.empty:
        return pd.DataFrame()

    df = df.set_index("timestamp").sort_index()

    hourly = df.resample("h").agg({
        "open_price": "first",
        "high_price": "max",
        "low_price": "min",
        "close_price": "last",
        "volume": "sum",
    })

    if asset_type == "crypto":
        start_ts = pd.Timestamp(execution_date, tz="UTC")
        end_ts = start_ts + pd.Timedelta(hours=23)

        full_index = pd.date_range(
            start=start_ts,
            end=end_ts,
            freq="h",
            tz="UTC",
        )

        hourly = hourly.reindex(full_index)

        price_cols = ["open_price", "high_price", "low_price", "close_price"]

        # GUARD: opening price wajib ada
        if hourly.iloc[0][price_cols].isna().any():
            raise DataValidationError(
                f"Missing opening price at 00:00 for crypto asset={asset}"
            )

        # Aman untuk forward-fill setelah guard
        hourly[price_cols] = hourly[price_cols].ffill()
        hourly["volume"] = hourly["volume"].fillna(0)

        if hourly[price_cols].isna().any().any():
            raise DataValidationError(
                f"Unfillable gap for crypto asset={asset}"
            )

    # stock → jangan dipaksa 24 jam
    hourly = hourly.dropna(subset=["close_price"])

    hourly = hourly.reset_index().rename(columns={"index": "timestamp"})
    hourly["hour_key"] = hourly["timestamp"].dt.strftime("%Y%m%d%H")
    hourly["asset"] = asset

    return hourly
