from typing import Dict, List
import os


def load_active_assets() -> List[Dict[str, str]]:
     return [
        # Crypto
        {"symbol": "BTC-USD", "type": "crypto"},
        {"symbol": "ETH-USD", "type": "crypto"},
        {"symbol": "LTC-USD", "type": "crypto"},
        {"symbol": "XRP-USD", "type": "crypto"},
        {"symbol": "SOL-USD", "type": "crypto"},

        # Stock
        {"symbol": "AAPL", "type": "stock"},
        {"symbol": "MSFT", "type": "stock"},
        {"symbol": "GOOGL", "type": "stock"},
        {"symbol": "AMZN", "type": "stock"},
        {"symbol": "NVDA", "type": "stock"},
    ]


def get_storage_base_path() -> str:
    """
    Base path for analytics storage.
    """
    return os.getenv("STORAGE_BASE_PATH", "./data/analytics")
