from datetime import datetime, date
from typing import Dict

import pandas as pd

from common.config import get_storage_base_path


def write_pipeline_event(event: Dict) -> None:
    """
    Persist a single pipeline event to the data lake (append-only).

    Expected event keys:
    - pipeline_run_id
    - pipeline_name
    - event_type
    - step
    - execution_date
    - asset (optional)
    - asset_type (optional)
    - reason (optional)
    """

    # Enrich event with system metadata
    event_record = {
        **event,
        "event_time": datetime.utcnow(),
    }

    df = pd.DataFrame([event_record])

    base_path = get_storage_base_path()

    # Partition by execution_date (same pattern as fact tables)
    execution_date: date = event["execution_date"]

    path = (
        f"{base_path}/ops_pipeline_events/"
        f"date={execution_date}/events.parquet"
    )

    # Append-safe write (idempotency not required for ops events)
    df.to_parquet(
        path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )
