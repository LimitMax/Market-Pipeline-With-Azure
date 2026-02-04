from typing import List
import os


def load_active_assets() -> List[str]:
    """
    Load active asset symbols for the pipeline.

    Temporary implementation:
    - Hardcoded list
    - Can be replaced with YAML / DB later
    """
    return [
        "BTC-USD",
        "ETH-USD",
    ]


def get_storage_base_path() -> str:
    """
    Base path for analytics storage.
    """
    return os.getenv("STORAGE_BASE_PATH", "./data/analytics")
