import pandas as pd
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from common.config import get_storage_base_path
from storage.market_repository import get_fs

_WATERMARK_RELATIVE_PATH = "_watermark/market_daily_watermark.parquet"

def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["asset", "asset_type", "last_date", "updated_at"]
    )

def read_daily_watermark() -> pd.DataFrame:
    base_path = get_storage_base_path()
    path = f"{base_path}/{_WATERMARK_RELATIVE_PATH}"
    fs = get_fs()

    if fs is None and not Path(path).exists():
        return _empty_df()

    try:
        return pd.read_parquet(path, filesystem=fs)
    except Exception:
        # corrupted / first run safety
        return _empty_df()

def get_last_processed_date(asset: str) -> Optional[date]:
    df = read_daily_watermark()
    row = df[df["asset"] == asset]

    if row.empty:
        return None

    return row.iloc[0]["last_date"]

def update_daily_watermark(
    *,
    asset: str,
    asset_type: str,
    last_date: date,
) -> None:
    """
    Advance watermark ONLY after successful full-day write.
    Idempotent per asset.
    """
    df = read_daily_watermark()
    now = datetime.utcnow()

    new_row = {
        "asset": asset,
        "asset_type": asset_type,
        "last_date": last_date,
        "updated_at": now,
    }

    if df.empty:
        df = pd.DataFrame([new_row])
    else:
        df = df[df["asset"] != asset]
        df = pd.concat(
            [df, pd.DataFrame([new_row])],
            ignore_index=True,
        )

    base_path = get_storage_base_path()
    path = f"{base_path}/{_WATERMARK_RELATIVE_PATH}"
    fs = get_fs()

    if fs is None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        path,
        index=False,
        engine="pyarrow",
        filesystem=fs,
    )
