from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from Data_Center.cleaning import make_news_id, normalize_news_df, normalize_price_df, pack_news_text
from Data_Center.http_client import http_get_json, provider_check
from Data_Center.models import NEWS_COLUMNS, PRICE_COLUMNS, NewsProvider, PriceProvider, ProviderError


def raise_finnhub_error(data: Any) -> None:
    if isinstance(data, dict) and data.get("error"):
        raise ProviderError(str(data["error"]))


def validate_quote(data: Any) -> None:
    raise_finnhub_error(data)
    if not isinstance(data, dict) or "c" not in data:
        raise ProviderError("Finnhub did not return quote data.")


def validate_news(data: Any) -> None:
    raise_finnhub_error(data)
    if not isinstance(data, list):
        raise ProviderError("Finnhub did not return a news list.")


def check_price_api(key: str):
    return provider_check(
        lambda: http_get_json("https://finnhub.io/api/v1/quote", {"symbol": "AAPL", "token": key}),
        validate_quote,
    )


def fetch_price(ticker: str, interval: str, start: datetime, end: datetime, key: str) -> pd.DataFrame:
    resolution_map = {
        "1min": "1",
        "5min": "5",
        "15min": "15",
        "30min": "30",
        "60min": "60",
        "1day": "D",
        "1week": "W",
    }
    start_unix = int(start.replace(tzinfo=timezone.utc).timestamp())
    end_unix = int(end.replace(tzinfo=timezone.utc).timestamp())
    data = http_get_json(
        "https://finnhub.io/api/v1/stock/candle",
        {
            "symbol": ticker.upper(),
            "resolution": resolution_map[interval],
            "from": start_unix,
            "to": end_unix,
            "token": key,
        },
    )
    raise_finnhub_error(data)
    if data.get("s") == "no_data":
        return pd.DataFrame(columns=PRICE_COLUMNS)
    if data.get("s") != "ok":
        raise ProviderError(f"Finnhub candle status: {data.get('s')}")

    rows = []
    for ts, open_, high, low, close, volume in zip(
        data.get("t", []),
        data.get("o", []),
        data.get("h", []),
        data.get("l", []),
        data.get("c", []),
        data.get("v", []),
    ):
        rows.append(
            {
                "Timestamp": pd.to_datetime(ts, unit="s", utc=True).tz_localize(None),
                "Open": open_,
                "High": high,
                "Low": low,
                "Close": close,
                "Volume": volume,
            }
        )
    df = normalize_price_df(rows, ticker, interval)
    return df[(df["Timestamp"] >= start) & (df["Timestamp"] <= end)].reset_index(drop=True)


def check_news_api(key: str):
    return provider_check(
        lambda: http_get_json("https://finnhub.io/api/v1/news", {"category": "general", "token": key}),
        validate_news,
    )


def fetch_news(ticker: str, start: datetime, end: datetime, limit: int, key: str) -> pd.DataFrame:
    data = http_get_json(
        "https://finnhub.io/api/v1/company-news",
        {
            "symbol": ticker.upper(),
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
            "token": key,
        },
    )
    raise_finnhub_error(data)
    if not isinstance(data, list):
        return pd.DataFrame(columns=NEWS_COLUMNS)

    rows = []
    for item in data[:limit]:
        published = pd.to_datetime(item.get("datetime"), unit="s", utc=True).tz_localize(None)
        title = item.get("headline")
        url = item.get("url")
        body = item.get("summary") or ""
        rows.append(
            {
                "News_ID": make_news_id("finnhub", item.get("id"), url, published, title),
                "Timestamp": published,
                "Summary": "",
                "Full_Text": pack_news_text("Finnhub", title, body, url, item.get("source")),
            }
        )
    return normalize_news_df(rows)


PRICE_PROVIDER = PriceProvider(
    name="finnhub",
    display_name="Finnhub",
    key_envs=("FINNHUB_API_KEY",),
    supported_intervals=("1min", "5min", "15min", "30min", "60min", "1day", "1week"),
    check=check_price_api,
    fetch=fetch_price,
    note="Candles and news share the same token.",
)

NEWS_PROVIDER = NewsProvider(
    name="finnhub",
    display_name="Finnhub Company News",
    key_envs=("FINNHUB_NEWS_API_KEY", "FINNHUB_API_KEY"),
    check=check_news_api,
    fetch=fetch_news,
    note="Company-news endpoint, good for ticker-specific headlines.",
)
