from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from Data_Center.cleaning import make_news_id, normalize_news_df, pack_news_text
from Data_Center.http_client import http_get_json, provider_check
from Data_Center.models import NewsProvider, ProviderError


def raise_marketaux_error(data: Any) -> None:
    if not isinstance(data, dict):
        raise ProviderError("Marketaux returned a non-object response.")
    if data.get("error"):
        raise ProviderError(str(data["error"]))


def validate_news(data: Any) -> None:
    raise_marketaux_error(data)
    if "data" not in data:
        raise ProviderError("Marketaux did not return news data.")


def check_news_api(key: str):
    return provider_check(
        lambda: http_get_json(
            "https://api.marketaux.com/v1/news/all",
            {"symbols": "AAPL", "limit": 1, "api_token": key},
        ),
        validate_news,
    )


def fetch_news(ticker: str, start: datetime, end: datetime, limit: int, key: str) -> pd.DataFrame:
    data = http_get_json(
        "https://api.marketaux.com/v1/news/all",
        {
            "symbols": ticker.upper(),
            "filter_entities": "true",
            "language": "en",
            "published_after": start.replace(tzinfo=timezone.utc).isoformat(),
            "published_before": end.replace(tzinfo=timezone.utc).isoformat(),
            "limit": max(1, min(limit, 50)),
            "api_token": key,
        },
    )
    raise_marketaux_error(data)
    rows = []
    for item in data.get("data") or []:
        title = item.get("title")
        url = item.get("url")
        source = (item.get("source") or {}).get("name") if isinstance(item.get("source"), dict) else item.get("source")
        body = item.get("description") or item.get("snippet") or ""
        rows.append(
            {
                "News_ID": make_news_id("marketaux", item.get("uuid"), url, item.get("published_at"), title),
                "Timestamp": item.get("published_at"),
                "Summary": "",
                "Full_Text": pack_news_text("Marketaux", title, body, url, source),
            }
        )
    return normalize_news_df(rows)


NEWS_PROVIDER = NewsProvider(
    name="marketaux",
    display_name="Marketaux",
    key_envs=("MARKETAUX_API_KEY",),
    check=check_news_api,
    fetch=fetch_news,
    note="Finance-news focused; free plan currently limits article count per request.",
)
