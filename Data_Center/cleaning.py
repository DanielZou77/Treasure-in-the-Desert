from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

import pandas as pd

from Data_Center.models import NEWS_COLUMNS, PRICE_COLUMNS


def parse_alpha_timestamp(value: str) -> datetime:
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def parse_datetime_input(value: str, *, is_end: bool = False) -> datetime | None:
    value = value.strip()
    if not value:
        return None
    normalized = value.replace("/", "-").replace("T", " ")
    if len(normalized) == 10:
        parsed = datetime.fromisoformat(normalized)
        if is_end:
            return parsed.replace(hour=23, minute=59, second=59)
        return parsed
    return datetime.fromisoformat(normalized)


def format_api_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_alpha_news_time(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M")


def normalize_price_df(rows: list[dict[str, Any]], ticker: str, interval: str) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce").dt.tz_localize(None)
    df["Ticker"] = ticker.upper()
    df["Interval"] = interval
    for col in ("Open", "High", "Low", "Close", "Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["Timestamp", "Open", "High", "Low", "Close"])
    df["Volume"] = df["Volume"].fillna(0).round().astype("int64")
    df = df[PRICE_COLUMNS].sort_values("Timestamp").drop_duplicates(
        ["Timestamp", "Ticker", "Interval"], keep="last"
    )
    return df.reset_index(drop=True)


def make_news_id(provider: str, *parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"{provider}_{digest}"


def normalize_news_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=NEWS_COLUMNS)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce").dt.tz_localize(None)
    df["Summary"] = df["Summary"].fillna("")
    df["Full_Text"] = df["Full_Text"].fillna("")
    df = df.dropna(subset=["News_ID", "Timestamp"])
    return df[NEWS_COLUMNS].sort_values("Timestamp").drop_duplicates("News_ID", keep="last")


def pack_news_text(
    provider: str,
    title: str | None,
    body: str | None,
    url: str | None,
    source: str | None,
) -> str:
    parts = [
        f"Provider: {provider}",
        f"Title: {title or ''}",
        f"Source: {source or ''}",
        f"URL: {url or ''}",
        "",
        body or "",
    ]
    return "\n".join(parts).strip()
