from typing import Dict, List
from datetime import date, datetime, timedelta
import pandas as pd
import yfinance as yf

from common.errors import SourceError


def extract_market_data(
    assets: List[str],
    execution_date: date,
    pipeline_run_id: str,
) -> Dict[str, pd.DataFrame]:

    results: Dict[str, pd.DataFrame] = {}

    for asset in assets:
        try:
            df = _fetch_single_asset(asset, execution_date)
            results[asset] = df

        except Exception as err:
            # Wrap any external failure as SourceError
            raise SourceError(
                f"Failed to fetch data from yfinance for asset={asset} "
                f"on execution_date={execution_date}: {err}"
            )

    return results


def _fetch_single_asset(
    asset: str,
    execution_date: date,
) -> pd.DataFrame:

    start_dt = datetime.combine(execution_date, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)

    ticker = yf.Ticker(asset)

    # yfinance returns index as DatetimeIndex
    df = ticker.history(
        start=start_dt,
        end=end_dt,
        interval="1h",
        auto_adjust=False,
        actions=False,
    )

    if df is None or df.empty:
        raise SourceError(
            f"Empty response from yfinance for asset={asset} "
            f"on execution_date={execution_date}"
        )

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
