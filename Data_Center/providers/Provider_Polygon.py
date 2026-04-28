from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from Data_Center.cleaning import normalize_price_df
from Data_Center.http_client import http_get_json, provider_check
from Data_Center.models import PriceProvider, ProviderError


def raise_polygon_error(data: Any) -> None:
    if not isinstance(data, dict):
        raise ProviderError("Polygon returned a non-object response.")
    status = str(data.get("status", "")).upper()
    if status in {"ERROR", "NOT_AUTHORIZED", "AUTH_ERROR"} or data.get("error"):
        raise ProviderError(str(data.get("error") or data.get("message") or data))


def validate_prev(data: Any) -> None:
    raise_polygon_error(data)
    if "results" not in data:
        raise ProviderError("Polygon did not return aggregate results.")


def check_price_api(key: str):
    return provider_check(
        lambda: http_get_json(
            "https://api.polygon.io/v2/aggs/ticker/AAPL/prev",
            {"adjusted": "true", "apiKey": key},
        ),
        validate_prev,
    )


def fetch_price(ticker: str, interval: str, start: datetime, end: datetime, key: str) -> pd.DataFrame:
    interval_map = {
        "1min": (1, "minute"),
        "5min": (5, "minute"),
        "15min": (15, "minute"),
        "30min": (30, "minute"),
        "60min": (1, "hour"),
        "1day": (1, "day"),
        "1week": (1, "week"),
    }
    multiplier, timespan = interval_map[interval]
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker.upper()}/range/"
        f"{multiplier}/{timespan}/{start:%Y-%m-%d}/{end:%Y-%m-%d}"
    )
    data = http_get_json(
        url,
        {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": key},
    )
    raise_polygon_error(data)
    rows = [
        {
            "Timestamp": pd.to_datetime(item.get("t"), unit="ms", utc=True).tz_localize(None),
            "Open": item.get("o"),
            "High": item.get("h"),
            "Low": item.get("l"),
            "Close": item.get("c"),
            "Volume": item.get("v"),
        }
        for item in data.get("results") or []
    ]
    df = normalize_price_df(rows, ticker, interval)
    return df[(df["Timestamp"] >= start) & (df["Timestamp"] <= end)].reset_index(drop=True)


PRICE_PROVIDER = PriceProvider(
    name="polygon",
    display_name="Polygon.io",
    key_envs=("POLYGON_API_KEY",),
    supported_intervals=("1min", "5min", "15min", "30min", "60min", "1day", "1week"),
    check=check_price_api,
    fetch=fetch_price,
    note="Aggregate bars are strong for US equities; free plan access can be delayed or limited.",
)
