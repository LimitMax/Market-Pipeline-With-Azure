from typing import Dict
import pandas as pd

from common.errors import DataValidationError


REQUIRED_COLUMNS = {
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
}


def clean_market_data(raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    cleaned_data: Dict[str, pd.DataFrame] = {}

    for asset, df in raw_data.items():
        if df is None or df.empty:
            raise DataValidationError(f"Empty raw dataframe for asset={asset}")

        _validate_required_columns(df, asset)

        df_clean = df.copy()

        # 1. Standardize column names
        df_clean = _standardize_columns(df_clean)

        # 2. Enforce data types
        df_clean = _cast_types(df_clean, asset)

        # 3. Drop exact duplicate rows (raw-level safety)
        df_clean = df_clean.drop_duplicates()

        # 4. Sort by timestamp (important for downstream logic)
        df_clean = df_clean.sort_values("timestamp").reset_index(drop=True)

        cleaned_data[asset] = df_clean

    return cleaned_data


def _validate_required_columns(df: pd.DataFrame, asset: str) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns.str.lower())
    if missing:
        raise DataValidationError(
            f"Missing required columns {missing} for asset={asset}"
        )


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to standard internal naming.
    """
    rename_map = {
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
    }

    df = df.rename(columns={c: c.lower() for c in df.columns})
    df = df.rename(columns=rename_map)

    return df


def _cast_types(df: pd.DataFrame, asset: str) -> pd.DataFrame:
    """
    Cast columns to expected dtypes.
    """
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        numeric_columns = [
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
        ]

        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="raise")

    except Exception as err:
        raise DataValidationError(
            f"Type casting failed for asset={asset}: {err}"
        )

    return df
