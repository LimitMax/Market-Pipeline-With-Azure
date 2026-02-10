import os
import pandas as pd

from pathlib import Path
from typing import Dict

from common.errors import SystemError
from common.config import get_storage_base_path

_fs = None

def get_fs():
    env = os.getenv("ENV")

    if env is None:
        raise SystemError("ENV is not set. Use ENV=local or ENV=cloud")

    if env == "local":
        return None 

    if env != "cloud":
        raise SystemError(f"Invalid ENV value: {env}")

    # Lazy import: hanya saat cloud
    global _fs
    if _fs is None:
        from adlfs.spec import AzureBlobFileSystem
        from azure.identity import DefaultAzureCredential

        _fs = AzureBlobFileSystem(
            account_name="marketpipeline",
            credential=DefaultAzureCredential(),
        )
    return _fs


def write_fact_market_hourly(
    hourly_data: Dict[str, pd.DataFrame],
    pipeline_run_id: str,
) -> None:
    base_path = get_storage_base_path()

    # local tidak boleh pakai Azure path
    if os.getenv("ENV") == "local" and base_path.startswith("abfs://"):
        raise SystemError(f"Local ENV cannot use Azure path: {base_path}")

    fs = get_fs()

    for asset, df in hourly_data.items():
        if df is None or df.empty:
            continue  # skip empty (stock non-trading)

        df = df.copy()
        df["date"] = df["timestamp"].dt.date.astype(str)
        date_value = df["date"].iloc[0]

        target_path = (
            f"{base_path}/fact_market_hourly/"
            f"asset={asset}/"
            f"date={date_value}/"
            f"data.parquet"
        )

        # Local FS perlu mkdir
        if fs is None:
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)

        try:
            df.to_parquet(
                target_path,
                index=False,
                engine="pyarrow",
                filesystem=fs,
            )
        except Exception as err:
            raise SystemError(
                f"Failed to write hourly data for asset={asset}: {err}"
            )