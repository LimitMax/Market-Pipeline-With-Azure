from datetime import date, timedelta
from typing import List
from common.config import load_active_assets
from storage.watermark_repository import get_last_processed_date

def determine_execution_dates(today: date) -> List[date]:
    """
    Determine all dates that need processing (auto catch-up).
    """
    assets = load_active_assets()

    next_dates = []

    for asset in assets:
        symbol = asset["symbol"]
        last_date = get_last_processed_date(symbol)

        if last_date is None:
            next_dates.append(today)
        else:
            next_dates.append(last_date + timedelta(days=1))

    start_date = min(next_dates)

    dates = []
    d = start_date
    while d <= today:
        dates.append(d)
        d += timedelta(days=1)

    return dates
