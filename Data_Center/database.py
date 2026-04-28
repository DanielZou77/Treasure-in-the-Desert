from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from Data_Center.config import db_path_from_env
from Data_Center.models import NEWS_COLUMNS, PRICE_COLUMNS


def connect_db(env: dict[str, str]) -> tuple[duckdb.DuckDBPyConnection, Path]:
    db_path = db_path_from_env(env)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    ensure_schema(con)
    return con, db_path


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS news_impact (
            News_ID VARCHAR,
            Ticker VARCHAR,
            Impact INTEGER CHECK (Impact IN (1, 0, -1))
        );
        CREATE TABLE IF NOT EXISTS news_master (
            News_ID VARCHAR PRIMARY KEY,
            Timestamp TIMESTAMP,
            Summary TEXT,
            Full_Text TEXT
        );
        CREATE TABLE IF NOT EXISTS price_volume (
            Timestamp TIMESTAMP,
            Ticker VARCHAR,
            Interval VARCHAR,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Volume BIGINT
        );
        CREATE TABLE IF NOT EXISTS factor_values (
            Timestamp TIMESTAMP,
            Ticker VARCHAR,
            Interval VARCHAR,
            Factor_Name VARCHAR,
            Factor_Value DOUBLE
        );
        CREATE TABLE IF NOT EXISTS factor_master (
            Factor_Name VARCHAR PRIMARY KEY,
            Factor_Formula TEXT,
            Generation_Time TIMESTAMP,
            Historical_Sharpe DOUBLE
        );
        """
    )
    ensure_column(con, "price_volume", "Interval", "VARCHAR", "'unknown'")
    ensure_column(con, "factor_values", "Interval", "VARCHAR", "'unknown'")


def ensure_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    column_type: str,
    default_sql: str,
) -> None:
    columns = {row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()}
    if column_name not in columns:
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type} DEFAULT {default_sql}")


def insert_price_rows(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    clean = df[PRICE_COLUMNS].copy()
    clean["Timestamp"] = pd.to_datetime(clean["Timestamp"], errors="coerce")
    clean = clean.dropna(subset=["Timestamp", "Ticker", "Interval"])
    clean = clean.drop_duplicates(["Timestamp", "Ticker", "Interval"], keep="last")
    if clean.empty:
        return 0

    before = con.execute("SELECT COUNT(*) FROM price_volume").fetchone()[0]
    con.register("incoming_price", clean)
    con.execute(
        """
        INSERT INTO price_volume (Timestamp, Ticker, Interval, Open, High, Low, Close, Volume)
        SELECT i.Timestamp, i.Ticker, i.Interval, i.Open, i.High, i.Low, i.Close, i.Volume
        FROM incoming_price i
        WHERE NOT EXISTS (
            SELECT 1
            FROM price_volume p
            WHERE p.Timestamp = i.Timestamp
              AND p.Ticker = i.Ticker
              AND p.Interval = i.Interval
        )
        """
    )
    con.unregister("incoming_price")
    after = con.execute("SELECT COUNT(*) FROM price_volume").fetchone()[0]
    return int(after - before)


def insert_news_rows(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    clean = df[NEWS_COLUMNS].copy()
    clean["Timestamp"] = pd.to_datetime(clean["Timestamp"], errors="coerce")
    clean = clean.dropna(subset=["News_ID", "Timestamp"])
    clean = clean.drop_duplicates("News_ID", keep="last")
    if clean.empty:
        return 0

    before = con.execute("SELECT COUNT(*) FROM news_master").fetchone()[0]
    con.register("incoming_news", clean)
    con.execute(
        """
        INSERT INTO news_master (News_ID, Timestamp, Summary, Full_Text)
        SELECT i.News_ID, i.Timestamp, i.Summary, i.Full_Text
        FROM incoming_news i
        WHERE NOT EXISTS (
            SELECT 1 FROM news_master n WHERE n.News_ID = i.News_ID
        )
        """
    )
    con.unregister("incoming_news")
    after = con.execute("SELECT COUNT(*) FROM news_master").fetchone()[0]
    return int(after - before)
