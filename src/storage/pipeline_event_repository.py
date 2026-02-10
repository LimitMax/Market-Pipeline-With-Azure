import os
import pandas as pd

from datetime import datetime, date
from uuid import uuid4
from typing import Dict
from pathlib import Path

from common.config import get_storage_base_path
from storage.market_repository import get_fs  
from common.errors import SystemError

def write_pipeline_event(event: Dict) -> None:
    event_time = datetime.utcnow()
    event_record = {**event, "event_time": event_time}
    df = pd.DataFrame([event_record])

    base_path = get_storage_base_path()
    execution_date: date = event["execution_date"]

    event_type = event.get("event_type", "UNKNOWN")
    run_id = (
        event.get("pipeline_run_id")
        or event.get("backfill_id")
        or "unknown"
    )

    filename = f"event_{event_time.strftime('%H%M%S_%f')}_{uuid4().hex}.parquet"

    path = (
        f"{base_path}/ops_pipeline_events/"
        f"date={execution_date}/"
        f"event_type={event_type}/"
        f"run_id={run_id}/"
        f"{filename}"
    )

    fs = get_fs()

    if fs is None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        path,
        engine="pyarrow",
        compression="snappy",
        index=False,
        filesystem=fs,
    )