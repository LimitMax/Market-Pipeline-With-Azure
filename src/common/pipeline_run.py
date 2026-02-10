import uuid
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def generate_run_id() -> str:
    return str(uuid.uuid4())

def start_pipeline_run(
    pipeline_run_id: str,
    pipeline_name: str,
    run_type: str,
    execution_date,
):
    logger.info(
    "Pipeline run started",
        extra={
            "event": "PIPELINE_RUN_START",
            "run_id": pipeline_run_id,
            "pipeline": pipeline_name,
            "run_type": run_type,
            "execution_date": str(execution_date),
            "start_time": _now(),
        },
    )

def complete_pipeline_run(
    pipeline_run_id: str,
    status: str,
):
    logger.info(
        "Pipeline run completed",
        extra={
            "event": "PIPELINE_RUN_COMPLETE",
            "run_id": pipeline_run_id,
            "status": status,
            "end_time": _now(),
        },
    )

def _now():
    return datetime.utcnow().isoformat()
