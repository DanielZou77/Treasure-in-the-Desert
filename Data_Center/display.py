from __future__ import annotations

import duckdb
import pandas as pd

from Data_Center.models import FACTOR_COLUMNS, NEWS_COLUMNS, PRICE_COLUMNS


def print_df(title: str, df: pd.DataFrame, columns: list[str]) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("-" * 88)
    if df.empty:
        print("(empty)")
        print("Columns: " + " | ".join(columns))
        return
    display = df.copy()
    for col in display.columns:
        if pd.api.types.is_datetime64_any_dtype(display[col]):
            display[col] = display[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    print(display.to_string(index=False, max_colwidth=70))


def print_all_tables(con: duckdb.DuckDBPyConnection, max_rows: int = 20) -> None:
    tables = [
        (
            "表1：新闻影响关联表 (news_impact)",
            "SELECT News_ID, Ticker, Impact FROM news_impact ORDER BY News_ID, Ticker LIMIT ?",
            ["News_ID", "Ticker", "Impact"],
        ),
        (
            "表2：新闻主表 (news_master)",
            "SELECT News_ID, Timestamp, Summary, Full_Text FROM news_master ORDER BY Timestamp DESC LIMIT ?",
            NEWS_COLUMNS,
        ),
        (
            "表3：量价基础表 (price_volume)",
            """
            SELECT Timestamp, Ticker, Interval, Open, High, Low, Close, Volume
            FROM price_volume
            ORDER BY Ticker, Interval, Timestamp DESC
            LIMIT ?
            """,
            PRICE_COLUMNS,
        ),
        (
            "表4：动态因子数值表 (factor_values)",
            """
            SELECT Timestamp, Ticker, Interval, Factor_Name, Factor_Value
            FROM factor_values
            ORDER BY Ticker, Interval, Timestamp DESC, Factor_Name
            LIMIT ?
            """,
            FACTOR_COLUMNS,
        ),
        (
            "表5：因子元数据主表 (factor_master)",
            """
            SELECT Factor_Name, Factor_Formula, Generation_Time, Historical_Sharpe
            FROM factor_master
            ORDER BY Factor_Name
            LIMIT ?
            """,
            ["Factor_Name", "Factor_Formula", "Generation_Time", "Historical_Sharpe"],
        ),
    ]
    for title, query, columns in tables:
        df = con.execute(query, (max_rows,)).df()
        if df.empty:
            df = pd.DataFrame(columns=columns)
        print_df(title, df, columns)
