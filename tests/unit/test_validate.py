import pandas as pd
import pytest
from datetime import date

from processing.validate import (
    validate_raw_data,
    validate_hourly_data,
)
from common.errors import DataValidationError


def test_validate_raw_data_success():
    raw_data = {
        "BTC-USD": {"dummy": "data"},
        "ETH-USD": {"dummy": "data"},
    }
    expected_assets = ["BTC-USD", "ETH-USD"]

    validate_raw_data(
        raw_data=raw_data,
        expected_assets=expected_assets,
        execution_date=date(2025, 2, 1),
    )

def test_validate_raw_data_missing_asset():
    raw_data = {
        "BTC-USD": {"dummy": "data"},
    }
    expected_assets = ["BTC-USD", "ETH-USD"]

    with pytest.raises(DataValidationError):
        validate_raw_data(
            raw_data=raw_data,
            expected_assets=expected_assets,
            execution_date=date(2025, 2, 1),
        )

def test_validate_raw_data_empty():
    with pytest.raises(DataValidationError):
        validate_raw_data(
            raw_data={},
            expected_assets=["BTC-USD"],
            execution_date=date(2025, 2, 1),
        )

def make_valid_hourly_df():
    return pd.DataFrame({
        "hour_key": [f"202502010{h}" for h in range(24)],
        "open_price": [100.0] * 24,
        "high_price": [110.0] * 24,
        "low_price": [90.0] * 24,
        "close_price": [105.0] * 24,
        "volume": [1000.0] * 24,
    })

def test_validate_hourly_data_success():
    hourly_data = {
        "BTC-USD": make_valid_hourly_df()
    }

    validate_hourly_data(
        hourly_data=hourly_data,
        execution_date=date(2025, 2, 1),
    )

def test_validate_hourly_data_duplicate_hour():
    df = make_valid_hourly_df()
    df.loc[23, "hour_key"] = df.loc[0, "hour_key"]

    hourly_data = {"BTC-USD": df}

    with pytest.raises(DataValidationError):
        validate_hourly_data(
            hourly_data=hourly_data,
            execution_date=date(2025, 2, 1),
        )

def test_validate_hourly_data_missing_hour():
    df = make_valid_hourly_df().iloc[:-1]  # remove one hour

    hourly_data = {"BTC-USD": df}

    with pytest.raises(DataValidationError):
        validate_hourly_data(
            hourly_data=hourly_data,
            execution_date=date(2025, 2, 1),
        )

def test_validate_hourly_data_invalid_price():
    df = make_valid_hourly_df()
    df.loc[0, "open_price"] = -10

    hourly_data = {"BTC-USD": df}

    with pytest.raises(DataValidationError):
        validate_hourly_data(
            hourly_data=hourly_data,
            execution_date=date(2025, 2, 1),
        )

def test_validate_hourly_data_negative_volume():
    df = make_valid_hourly_df()
    df.loc[0, "volume"] = -1

    hourly_data = {"BTC-USD": df}

    with pytest.raises(DataValidationError):
        validate_hourly_data(
            hourly_data=hourly_data,
            execution_date=date(2025, 2, 1),
        )
