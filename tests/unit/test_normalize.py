import pandas as pd
import pytest
from datetime import date, datetime, timezone

from processing.normalisasi import normalize_to_hourly
from common.errors import DataValidationError

def make_cleaned_df_full_hours():
    timestamps = pd.date_range(
        start="2025-02-01 00:00:00",
        periods=24,
        freq="H",
        tz="UTC",
    )

    return pd.DataFrame({
        "timestamp": timestamps,
        "open_price": [100.0] * 24,
        "high_price": [110.0] * 24,
        "low_price": [90.0] * 24,
        "close_price": [105.0] * 24,
        "volume": [1000.0] * 24,
    })

def make_cleaned_df_missing_hours():
    timestamps = pd.date_range(
        start="2025-02-01 00:00:00",
        periods=20,
        freq="H",
        tz="UTC",
    )

    return pd.DataFrame({
        "timestamp": timestamps,
        "open_price": [100.0] * 20,
        "high_price": [110.0] * 20,
        "low_price": [90.0] * 20,
        "close_price": [105.0] * 20,
        "volume": [1000.0] * 20,
    })

def test_normalize_hourly_success_full_hours():
    cleaned_data = {
        "BTC-USD": make_cleaned_df_full_hours()
    }

    result = normalize_to_hourly(
        cleaned_data=cleaned_data,
        execution_date=date(2025, 2, 1),
    )

    hourly_df = result["BTC-USD"]

    assert len(hourly_df) == 24
    assert hourly_df["hour_key"].is_unique
    assert not hourly_df.isnull().any().any()
    assert not hourly_df["data_gap_flag"].any()

def test_normalize_hourly_missing_hours_filled():
    cleaned_data = {
        "BTC-USD": make_cleaned_df_missing_hours()
    }

    result = normalize_to_hourly(
        cleaned_data=cleaned_data,
        execution_date=date(2025, 2, 1),
    )

    hourly_df = result["BTC-USD"]

    assert len(hourly_df) == 24
    assert hourly_df["data_gap_flag"].any()

def test_normalize_hourly_handles_duplicate_raw_rows():
    df = make_cleaned_df_full_hours()
    df = pd.concat([df, df.iloc[[0]]])  # duplicate first row

    cleaned_data = {"BTC-USD": df}

    result = normalize_to_hourly(
        cleaned_data=cleaned_data,
        execution_date=date(2025, 2, 1),
    )

    hourly_df = result["BTC-USD"]

    assert hourly_df["hour_key"].is_unique
    assert len(hourly_df) == 24

def test_normalize_hourly_unfillable_gap_fails():
    df = make_cleaned_df_full_hours()
    df.loc[:, ["open_price", "high_price", "low_price", "close_price"]] = None

    cleaned_data = {"BTC-USD": df}

    with pytest.raises(DataValidationError):
        normalize_to_hourly(
            cleaned_data=cleaned_data,
            execution_date=date(2025, 2, 1),
        )
