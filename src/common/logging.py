import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)


def log_pipeline_start(pipeline_name: str, run_id: str, execution_date):
    logging.info({
        "event": "PIPELINE_START",
        "pipeline": pipeline_name,
        "run_id": run_id,
        "execution_date": str(execution_date),
        "timestamp": _now(),
    })


def log_pipeline_end(pipeline_name: str, run_id: str):
    logging.info({
        "event": "PIPELINE_END",
        "pipeline": pipeline_name,
        "run_id": run_id,
        "timestamp": _now(),
    })


def log_error(
    pipeline_run_id: str,
    step: str,
    error_type: str,
    error: Exception,
):
    logging.error({
        "event": "PIPELINE_ERROR",
        "run_id": pipeline_run_id,
        "step": step,
        "error_type": error_type,
        "error_message": str(error),
        "timestamp": _now(),
    })


def _now():
    return datetime.utcnow().isoformat()
