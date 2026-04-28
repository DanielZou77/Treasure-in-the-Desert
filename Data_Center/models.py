from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd


PRICE_COLUMNS = [
    "Timestamp",
    "Ticker",
    "Interval",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
]
FACTOR_COLUMNS = ["Timestamp", "Ticker", "Interval", "Factor_Name", "Factor_Value"]
NEWS_COLUMNS = ["News_ID", "Timestamp", "Summary", "Full_Text"]

INTERVAL_CHOICES = {
    "1": ("1min", "1 minute"),
    "2": ("5min", "5 minutes"),
    "3": ("15min", "15 minutes"),
    "4": ("30min", "30 minutes"),
    "5": ("60min", "1 hour"),
    "6": ("1day", "1 day"),
    "7": ("1week", "1 week"),
}

INTERVAL_DELTAS = {
    "1min": timedelta(minutes=1),
    "5min": timedelta(minutes=5),
    "15min": timedelta(minutes=15),
    "30min": timedelta(minutes=30),
    "60min": timedelta(hours=1),
    "1day": timedelta(days=1),
    "1week": timedelta(weeks=1),
}


class ProviderError(Exception):
    """Raised when an upstream API returns an error or malformed payload."""


@dataclass(frozen=True)
class ApiCheckResult:
    ok: bool
    message: str


@dataclass(frozen=True)
class PriceProvider:
    name: str
    display_name: str
    key_envs: tuple[str, ...]
    supported_intervals: tuple[str, ...]
    check: Callable[[str], ApiCheckResult]
    fetch: Callable[[str, str, datetime, datetime, str], pd.DataFrame]
    note: str


@dataclass(frozen=True)
class NewsProvider:
    name: str
    display_name: str
    key_envs: tuple[str, ...]
    check: Callable[[str], ApiCheckResult]
    fetch: Callable[[str, datetime, datetime, int, str], pd.DataFrame]
    note: str
