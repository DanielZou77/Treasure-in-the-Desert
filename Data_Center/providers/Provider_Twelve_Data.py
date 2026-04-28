from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from Data_Center.cleaning import format_api_datetime, normalize_price_df
from Data_Center.http_client import http_get_json, provider_check
from Data_Center.models import PriceProvider, ProviderError


def raise_twelve_error(data: Any) -> None:
    if not isinstance(data, dict):
        raise ProviderError("Twelve Data returned a non-object response.")
    if data.get("status") == "error":
        raise ProviderError(str(data.get("message", "Twelve Data error")))
    if "code" in data and "message" in data and "values" not in data:
        raise ProviderError(str(data.get("message")))


def validate_timeseries(data: Any) -> None:
    raise_twelve_error(data)
    values = data.get("values") if isinstance(data, dict) else None
    if not isinstance(values, list):
        raise ProviderError("Twelve Data did not return time_series values.")


def check_price_api(key: str):
    return provider_check(
        lambda: http_get_json(
            "https://api.twelvedata.com/time_series",
            {"symbol": "AAPL", "interval": "1day", "outputsize": 1, "apikey": key},
        ),
        validate_timeseries,
    )


def fetch_price(ticker: str, interval: str, start: datetime, end: datetime, key: str) -> pd.DataFrame:
    interval_map = {
        "1min": "1min",
        "5min": "5min",
        "15min": "15min",
        "30min": "30min",
        "60min": "1h",
        "1day": "1day",
        "1week": "1week",
    }
    data = http_get_json(
        "https://api.twelvedata.com/time_series",
        {
            "symbol": ticker.upper(),
            "interval": interval_map[interval],
            "start_date": format_api_datetime(start),
            "end_date": format_api_datetime(end),
            "outputsize": 5000,
            "apikey": key,
        },
    )
    raise_twelve_error(data)
    rows = [
        {
            "Timestamp": item.get("datetime"),
            "Open": item.get("open"),
            "High": item.get("high"),
            "Low": item.get("low"),
            "Close": item.get("close"),
            "Volume": item.get("volume"),
        }
        for item in data.get("values") or []
    ]
    df = normalize_price_df(rows, ticker, interval)
    return df[(df["Timestamp"] >= start) & (df["Timestamp"] <= end)].reset_index(drop=True)


PRICE_PROVIDER = PriceProvider(
    name="twelve_data",
    display_name="Twelve Data",
    key_envs=("TWELVE_DATA_API_KEY",),
    supported_intervals=("1min", "5min", "15min", "30min", "60min", "1day", "1week"),
    check=check_price_api,
    fetch=fetch_price,
    note="Good unified time_series endpoint; free credits are limited.",
)
