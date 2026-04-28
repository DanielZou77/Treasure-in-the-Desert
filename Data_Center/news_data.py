from __future__ import annotations

from datetime import datetime

import duckdb

from Data_Center.database import insert_news_rows
from Data_Center.models import NewsProvider


def ingest_news(
    con: duckdb.DuckDBPyConnection,
    provider: NewsProvider,
    key: str,
    ticker: str,
    start: datetime,
    end: datetime,
    limit: int,
) -> int:
    news_df = provider.fetch(ticker, start, end, limit, key)
    return insert_news_rows(con, news_df)
