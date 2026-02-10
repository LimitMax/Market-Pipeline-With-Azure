from typing import Dict
import pandas as pd
import yfinance as yf

from datetime import date, datetime, timedelta
from common.errors import SourceError

def extract_market_data(
    symbol: str,
    asset_type: str,
    execution_date: date,
    pipeline_run_id: str,
) -> pd.DataFrame:
    
    try:
        df = _fetch_single_asset(symbol, execution_date)
    except Exception as err:
        if asset_type == "stock":
            # stock boleh kosong / gagal
            return pd.DataFrame()
        raise SourceError(
            f"Failed to fetch data from yfinance for asset={symbol} "
            f"on execution_date={execution_date}: {err}"
        )
    # Add minimal metadata for downstream steps
    df["asset"] = symbol
    df["asset_type"] = asset_type
    df["execution_date"] = execution_date
    df["pipeline_run_id"] = pipeline_run_id

    return df

def _fetch_single_asset(
    symbol: str,
    execution_date: date,
) -> pd.DataFrame:
    start_dt = datetime.combine(execution_date, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)

    ticker = yf.Ticker(symbol)

    df = ticker.history(
        start=start_dt,
        end=end_dt,
        interval="1h",
        auto_adjust=False,
        actions=False,
    )

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()

    # Standardize column naming early (raw-level consistency)
    df = df.rename(
        columns={
            "Datetime": "timestamp",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    return df
