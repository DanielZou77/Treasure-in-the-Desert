from __future__ import annotations

from datetime import datetime, timedelta

import duckdb
import pandas as pd

from Data_Center.database import insert_price_rows
from Data_Center.models import PRICE_COLUMNS, PriceProvider


def existing_price_stats(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    interval: str,
    start: datetime,
    end: datetime,
) -> tuple[int, datetime | None, datetime | None]:
    row = con.execute(
        """
        SELECT COUNT(*), MIN(Timestamp), MAX(Timestamp)
        FROM price_volume
        WHERE Ticker = ? AND Interval = ? AND Timestamp BETWEEN ? AND ?
        """,
        (ticker.upper(), interval, start, end),
    ).fetchone()
    return int(row[0]), row[1], row[2]


def derive_from_lower_timeframes(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    interval: str,
    start: datetime,
    end: datetime,
) -> int:
    if interval not in {"1day", "1week"}:
        return 0

    candidates = ["1min", "5min", "15min", "30min", "60min"]
    if interval == "1week":
        candidates.append("1day")

    for candidate in candidates:
        base = con.execute(
            """
            SELECT Timestamp, Ticker, Interval, Open, High, Low, Close, Volume
            FROM price_volume
            WHERE Ticker = ?
              AND Interval = ?
              AND Timestamp BETWEEN ? AND ?
            ORDER BY Timestamp
            """,
            (ticker.upper(), candidate, start - timedelta(days=7), end + timedelta(days=7)),
        ).df()
        if not base.empty:
            break
    else:
        return 0

    base["Timestamp"] = pd.to_datetime(base["Timestamp"], errors="coerce")
    base = base.dropna(subset=["Timestamp"]).sort_values("Timestamp")
    if interval == "1day":
        base["Bucket"] = base["Timestamp"].dt.floor("D")
    else:
        base["Bucket"] = base["Timestamp"].dt.to_period("W-SUN").dt.start_time

    grouped = (
        base.groupby("Bucket", as_index=False)
        .agg(
            Open=("Open", "first"),
            High=("High", "max"),
            Low=("Low", "min"),
            Close=("Close", "last"),
            Volume=("Volume", "sum"),
        )
        .rename(columns={"Bucket": "Timestamp"})
    )
    grouped["Ticker"] = ticker.upper()
    grouped["Interval"] = interval
    grouped = grouped[PRICE_COLUMNS]
    if interval == "1day":
        window_start = datetime(start.year, start.month, start.day)
    else:
        week_start = start - timedelta(days=start.weekday())
        window_start = datetime(week_start.year, week_start.month, week_start.day)
    grouped = grouped[(grouped["Timestamp"] >= window_start) & (grouped["Timestamp"] <= end)]
    return insert_price_rows(con, grouped)


def fetch_missing_price_ranges(
    con: duckdb.DuckDBPyConnection,
    provider: PriceProvider,
    key: str,
    ticker: str,
    interval: str,
    start: datetime,
    end: datetime,
) -> int:
    count, min_ts, max_ts = existing_price_stats(con, ticker, interval, start, end)
    if count and min_ts and max_ts and min_ts <= start and max_ts >= end:
        print(f"已有 {count} 条 {ticker} {interval} 覆盖目标区间，本次不调用行情 API。")
        return 0

    ranges: list[tuple[datetime, datetime]] = []
    if count == 0 or min_ts is None or max_ts is None:
        ranges.append((start, end))
    else:
        if min_ts > start:
            ranges.append((start, min_ts - timedelta(microseconds=1)))
        if max_ts < end:
            ranges.append((max_ts + timedelta(microseconds=1), end))

    inserted = 0
    for range_start, range_end in ranges:
        if range_start >= range_end:
            continue
        print(f"从 {provider.display_name} 拉取 {ticker} {interval}: {range_start} -> {range_end}")
        df = provider.fetch(ticker, interval, range_start, range_end, key)
        inserted += insert_price_rows(con, df)
    return inserted


def ingest_stock_bars(
    con: duckdb.DuckDBPyConnection,
    provider: PriceProvider,
    key: str,
    ticker: str,
    interval: str,
    start: datetime,
    end: datetime,
) -> tuple[int, int]:
    derived = derive_from_lower_timeframes(con, ticker, interval, start, end)
    inserted = fetch_missing_price_ranges(con, provider, key, ticker, interval, start, end)
    return derived, inserted
