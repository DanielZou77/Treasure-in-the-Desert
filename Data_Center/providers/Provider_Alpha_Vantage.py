from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from Data_Center.cleaning import (
    format_alpha_news_time,
    make_news_id,
    normalize_news_df,
    normalize_price_df,
    pack_news_text,
    parse_alpha_timestamp,
)
from Data_Center.http_client import http_get_json, provider_check
from Data_Center.models import NewsProvider, PriceProvider, ProviderError


def raise_alpha_error(data: Any) -> None:
    if not isinstance(data, dict):
        raise ProviderError("Alpha Vantage returned a non-object response.")
    for key in ("Error Message", "Note", "Information"):
        if key in data:
            raise ProviderError(str(data[key])[:300])


def validate_global_quote(data: Any) -> None:
    raise_alpha_error(data)
    quote = data.get("Global Quote") if isinstance(data, dict) else None
    if not isinstance(quote, dict) or not quote:
        raise ProviderError("Alpha Vantage did not return Global Quote data.")


def validate_news(data: Any) -> None:
    raise_alpha_error(data)
    if "feed" not in data:
        raise ProviderError("Alpha Vantage did not return a news feed.")


def check_price_api(key: str):
    return provider_check(
        lambda: http_get_json(
            "https://www.alphavantage.co/query",
            {"function": "GLOBAL_QUOTE", "symbol": "AAPL", "apikey": key},
        ),
        validate_global_quote,
    )


def fetch_price(ticker: str, interval: str, start: datetime, end: datetime, key: str) -> pd.DataFrame:
    interval_map = {
        "1min": ("TIME_SERIES_INTRADAY", "1min", "Time Series (1min)"),
        "5min": ("TIME_SERIES_INTRADAY", "5min", "Time Series (5min)"),
        "15min": ("TIME_SERIES_INTRADAY", "15min", "Time Series (15min)"),
        "30min": ("TIME_SERIES_INTRADAY", "30min", "Time Series (30min)"),
        "60min": ("TIME_SERIES_INTRADAY", "60min", "Time Series (60min)"),
        "1day": ("TIME_SERIES_DAILY", None, "Time Series (Daily)"),
        "1week": ("TIME_SERIES_WEEKLY", None, "Weekly Time Series"),
    }
    function, api_interval, payload_key = interval_map[interval]
    params = {"function": function, "symbol": ticker.upper(), "apikey": key}
    if api_interval:
        params["interval"] = api_interval
        params["outputsize"] = "full"
    elif interval == "1day":
        # 免费 key 的 daily full 会被拒，这里走 compact。
        params["outputsize"] = "compact"

    data = http_get_json("https://www.alphavantage.co/query", params)
    try:
        raise_alpha_error(data)
    except ProviderError as exc:
        if params.get("outputsize") == "full" and "outputsize=full" in str(exc):
            params["outputsize"] = "compact"
            data = http_get_json("https://www.alphavantage.co/query", params)
            raise_alpha_error(data)
        else:
            raise
    series = data.get(payload_key)
    if not isinstance(series, dict):
        raise ProviderError(f"Alpha Vantage response did not include {payload_key}.")

    rows = [
        {
            "Timestamp": ts,
            "Open": values.get("1. open"),
            "High": values.get("2. high"),
            "Low": values.get("3. low"),
            "Close": values.get("4. close"),
            "Volume": values.get("5. volume"),
        }
        for ts, values in series.items()
    ]
    df = normalize_price_df(rows, ticker, interval)
    return df[(df["Timestamp"] >= start) & (df["Timestamp"] <= end)].reset_index(drop=True)


def check_news_api(key: str):
    return provider_check(
        lambda: http_get_json(
            "https://www.alphavantage.co/query",
            {"function": "NEWS_SENTIMENT", "tickers": "AAPL", "limit": 1, "apikey": key},
        ),
        validate_news,
    )


def fetch_news(ticker: str, start: datetime, end: datetime, limit: int, key: str) -> pd.DataFrame:
    data = http_get_json(
        "https://www.alphavantage.co/query",
        {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker.upper(),
            "time_from": format_alpha_news_time(start),
            "time_to": format_alpha_news_time(end),
            "limit": max(1, min(limit, 1000)),
            "apikey": key,
        },
    )
    raise_alpha_error(data)
    rows = []
    for item in data.get("feed") or []:
        title = item.get("title")
        url = item.get("url")
        published = parse_alpha_timestamp(str(item.get("time_published", "")))
        body = item.get("summary") or item.get("overall_sentiment_label") or ""
        rows.append(
            {
                "News_ID": make_news_id("alpha", url, published, title),
                "Timestamp": published,
                "Summary": "",
                "Full_Text": pack_news_text("Alpha Vantage", title, body, url, item.get("source")),
            }
        )
    return normalize_news_df(rows)


PRICE_PROVIDER = PriceProvider(
    name="alpha_vantage",
    display_name="Alpha Vantage",
    key_envs=("ALPHA_VANTAGE_API_KEY",),
    supported_intervals=("1min", "5min", "15min", "30min", "60min", "1day", "1week"),
    check=check_price_api,
    fetch=fetch_price,
    note="Free tier is convenient, but rate limits are tight.",
)

NEWS_PROVIDER = NewsProvider(
    name="alpha_vantage",
    display_name="Alpha Vantage News Sentiment",
    key_envs=("ALPHA_VANTAGE_NEWS_API_KEY", "ALPHA_VANTAGE_API_KEY"),
    check=check_news_api,
    fetch=fetch_news,
    note="Ticker-aware finance news and sentiment.",
)
