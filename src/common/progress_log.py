import os
from typing import Optional

SHOW_PROGRESS_ENV = os.getenv("SHOW_PROGRESS", "false").lower() == "true"

def _should_show_progress(run_type: Optional[str] = None) -> bool:
    """
    Decide whether progress log should be shown.

    Rules:
    - backfill           -> ALWAYS show
    - local manual run   -> show if SHOW_PROGRESS=true
    - scheduled prod     -> hide
    """
    if run_type == "backfill":
        return True

    return SHOW_PROGRESS_ENV

def header(title: str, execution_date=None, run_type: Optional[str] = None):
    if not _should_show_progress(run_type):
        return

    print("=" * 50, flush=True)
    print(title, flush=True)
    if execution_date:
        print(f"Execution Date : {execution_date}", flush=True)
    if run_type:
        print(f"Run Type       : {run_type}", flush=True)
    print("=" * 50, flush=True)

def process(message: str, run_type: Optional[str] = None):
    if not _should_show_progress(run_type):
        return

    print(f"\n[PROCESS] {message}", flush=True)


def item(name: str, message: str, run_type: Optional[str] = None):
    if not _should_show_progress(run_type):
        return

    print(f"  - {name:<12}: {message}", flush=True)


def result(message: str, run_type: Optional[str] = None):
    if not _should_show_progress(run_type):
        return

    print(f"\n[RESULT] {message}", flush=True)
    print("=" * 50, flush=True)
