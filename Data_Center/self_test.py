from __future__ import annotations

import math
from datetime import datetime, timedelta

import duckdb

from Data_Center.cleaning import normalize_price_df
from Data_Center.database import ensure_schema, insert_price_rows
from Data_Center.features import calculate_and_store_features
from Data_Center.stock_data import derive_from_lower_timeframes


def run_self_test() -> None:
    con = duckdb.connect(":memory:")
    ensure_schema(con)
    base_time = datetime(2026, 1, 2, 9, 30)
    rows = []
    for idx in range(180):
        ts = base_time + timedelta(minutes=idx)
        price = 100 + idx * 0.05 + math.sin(idx / 8)
        rows.append(
            {
                "Timestamp": ts,
                "Ticker": "TEST",
                "Interval": "1min",
                "Open": price,
                "High": price + 0.8,
                "Low": price - 0.7,
                "Close": price + 0.2,
                "Volume": 1000 + idx,
            }
        )
    price_df = normalize_price_df(rows, "TEST", "1min")
    inserted = insert_price_rows(con, price_df)
    derived = derive_from_lower_timeframes(con, "TEST", "1day", base_time, base_time + timedelta(days=1))
    factor_count = calculate_and_store_features(con, "TEST", "1min")
    assert inserted == 180
    assert derived >= 1
    assert factor_count > 0
    print(f"SELF TEST OK: price={inserted}, derived_daily={derived}, factors={factor_count}")
    con.close()
