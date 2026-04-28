from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from Data_Center.cleaning import make_news_id, normalize_news_df, pack_news_text
from Data_Center.http_client import http_get_json, provider_check
from Data_Center.models import NewsProvider, ProviderError


def raise_newsapi_error(data: Any) -> None:
    if not isinstance(data, dict):
        raise ProviderError("NewsAPI returned a non-object response.")
    if data.get("status") == "error":
        raise ProviderError(str(data.get("message", "NewsAPI error")))


def validate_headlines(data: Any) -> None:
    raise_newsapi_error(data)
    if data.get("status") != "ok":
        raise ProviderError("NewsAPI status is not ok.")


def check_news_api(key: str):
    return provider_check(
        lambda: http_get_json(
            "https://newsapi.org/v2/top-headlines",
            {"country": "us", "pageSize": 1, "apiKey": key},
        ),
        validate_headlines,
    )


def fetch_news(ticker: str, start: datetime, end: datetime, limit: int, key: str) -> pd.DataFrame:
    data = http_get_json(
        "https://newsapi.org/v2/everything",
        {
            "q": f"{ticker.upper()} stock OR {ticker.upper()} earnings OR {ticker.upper()} market",
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": max(1, min(limit, 100)),
            "apiKey": key,
        },
    )
    raise_newsapi_error(data)
    rows = []
    for item in data.get("articles") or []:
        title = item.get("title")
        url = item.get("url")
        source = (item.get("source") or {}).get("name") if isinstance(item.get("source"), dict) else item.get("source")
        body = item.get("description") or item.get("content") or ""
        rows.append(
            {
                "News_ID": make_news_id("newsapi", url, item.get("publishedAt"), title),
                "Timestamp": item.get("publishedAt"),
                "Summary": "",
                "Full_Text": pack_news_text("NewsAPI", title, body, url, source),
            }
        )
    return normalize_news_df(rows)


NEWS_PROVIDER = NewsProvider(
    name="newsapi",
    display_name="NewsAPI",
    key_envs=("NEWSAPI_API_KEY",),
    check=check_news_api,
    fetch=fetch_news,
    note="Good general news search; free developer plan is for development/testing only.",
)
