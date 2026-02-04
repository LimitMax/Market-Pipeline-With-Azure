from typing import Dict
import pandas as pd

from common.errors import SystemError
from common.config import get_storage_base_path

from adlfs.spec import AzureBlobFileSystem

def write_fact_market_hourly(
    hourly_data: Dict[str, pd.DataFrame],
    pipeline_run_id: str,
) -> None:
    base_path = get_storage_base_path()

    for asset, df in hourly_data.items():
        try:
            _write_single_asset(df, asset, base_path)

        except Exception as err:
            raise SystemError(
                f"Failed to write hourly data for asset={asset}: {err}"
            )


def _write_single_asset(
    df: pd.DataFrame,
    asset: str,
    base_path: str,
) -> None:
    if df.empty:
        raise SystemError(f"Attempted to write empty dataframe for asset={asset}")

    df = df.copy()
    df["date"] = df["timestamp"].dt.date.astype(str)

    date_value = df["date"].iloc[0]

    target_path = (
        f"{base_path}/fact_market_hourly/"
        f"asset={asset}/"
        f"date={date_value}/"
        f"data.parquet"
    )

    # 🔑 CRITICAL FIX: explicit Azure filesystem
    fs = AzureBlobFileSystem(
        account_name=None,  # picked up from env
        account_key=None,   # picked up from env
    )

    df.to_parquet(
        target_path,
        index=False,
        engine="pyarrow",
        filesystem=fs,      # 👈 THIS IS THE KEY
    )
