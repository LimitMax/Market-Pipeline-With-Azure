import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def log_pipeline_start(pipeline_name: str, run_id: str, execution_date):
    logger.info(
    "Pipeline started",
        extra={
            "event": "PIPELINE_START",
            "run_id": run_id,
            "pipeline": pipeline_name,
            "execution_date": str(execution_date),
            "timestamp": _now(),
        },
    )

def log_pipeline_end(pipeline_name: str, run_id: str):
    logger.info(
    "Pipeline finished",
    extra={
        "event": "PIPELINE_END",
        "run_id": run_id,
        "pipeline": pipeline_name,
        "timestamp": _now(),
    },
)

def log_error(
    pipeline_run_id: str,
    step: str,
    error_type: str,
    error: Exception,
):
    logger.error(
    "Pipeline error",
        extra={
            "event": "PIPELINE_ERROR",
            "run_id": pipeline_run_id,
            "step": step,
            "error_type": error_type,
            "error_message": str(error),
            "timestamp": _now(),
        },
    )

def _now():
    return datetime.utcnow().isoformat()
