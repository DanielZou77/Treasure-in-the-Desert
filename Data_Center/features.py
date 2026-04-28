from __future__ import annotations

import math
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd

from Data_Center.models import FACTOR_COLUMNS


FACTOR_FORMULAS = {
    "return_1": "Close / Close[-1] - 1",
    "log_return_1": "ln(Close / Close[-1])",
    "rolling_return_5": "Close / Close[-5] - 1",
    "rolling_return_10": "Close / Close[-10] - 1",
    "rolling_return_20": "Close / Close[-20] - 1",
    "volatility_5": "std(return_1, 5)",
    "volatility_10": "std(return_1, 10)",
    "volatility_20": "std(return_1, 20)",
    "sma_5": "mean(Close, 5)",
    "sma_10": "mean(Close, 10)",
    "sma_20": "mean(Close, 20)",
    "sma_50": "mean(Close, 50)",
    "ema_12": "ema(Close, 12)",
    "ema_26": "ema(Close, 26)",
    "macd": "ema_12 - ema_26",
    "macd_signal": "ema(macd, 9)",
    "macd_hist": "macd - macd_signal",
    "rsi_14": "100 - 100 / (1 + avg_gain_14 / avg_loss_14)",
    "kdj_k": "100 * (Close - low_9) / (high_9 - low_9)",
    "kdj_d": "mean(kdj_k, 3)",
    "kdj_j": "3 * kdj_k - 2 * kdj_d",
    "atr_14": "mean(true_range, 14)",
    "boll_mid_20": "sma_20",
    "boll_upper_20": "sma_20 + 2 * std(Close, 20)",
    "boll_lower_20": "sma_20 - 2 * std(Close, 20)",
    "boll_width_20": "(boll_upper_20 - boll_lower_20) / boll_mid_20",
    "obv": "cumsum(sign(Close - Close[-1]) * Volume)",
    "volume_sma_20": "mean(Volume, 20)",
    "volume_ratio_20": "Volume / volume_sma_20",
    "hl_spread_pct": "(High - Low) / Close",
    "close_position_in_range": "(Close - Low) / (High - Low)",
    "momentum_10": "Close - Close[-10]",
    "roc_10": "100 * (Close / Close[-10] - 1)",
    "williams_r_14": "-100 * (high_14 - Close) / (high_14 - low_14)",
    "cci_20": "(typical_price - sma(typical_price, 20)) / (0.015 * mean_abs_dev_20)",
    "zscore_20": "(Close - sma_20) / std(Close, 20)",
}


def register_factor_formulas(con: duckdb.DuckDBPyConnection) -> None:
    now = datetime.now()
    for name, formula in FACTOR_FORMULAS.items():
        exists = con.execute(
            "SELECT COUNT(*) FROM factor_master WHERE Factor_Name = ?",
            (name,),
        ).fetchone()[0]
        if not exists:
            con.execute(
                """
                INSERT INTO factor_master
                    (Factor_Name, Factor_Formula, Generation_Time, Historical_Sharpe)
                VALUES (?, ?, ?, ?)
                """,
                (name, formula, now, None),
            )


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0, np.nan)


def compute_feature_frame(price_df: pd.DataFrame) -> pd.DataFrame:
    if price_df.empty:
        return pd.DataFrame(columns=FACTOR_COLUMNS)

    df = price_df.sort_values("Timestamp").copy()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"].fillna(0)
    prev_close = close.shift(1)
    returns = close.pct_change()

    ema_12 = close.ewm(span=12, adjust=False).mean()
    ema_26 = close.ewm(span=26, adjust=False).mean()
    macd = ema_12 - ema_26
    macd_signal = macd.ewm(span=9, adjust=False).mean()

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = safe_divide(gain, loss)

    low_9 = low.rolling(9).min()
    high_9 = high.rolling(9).max()
    kdj_k = 100 * safe_divide(close - low_9, high_9 - low_9)
    kdj_d = kdj_k.rolling(3).mean()

    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    sma_20 = close.rolling(20).mean()
    std_20 = close.rolling(20).std()
    boll_upper = sma_20 + 2 * std_20
    boll_lower = sma_20 - 2 * std_20

    direction = np.sign(close.diff()).fillna(0)
    high_14 = high.rolling(14).max()
    low_14 = low.rolling(14).min()
    typical_price = (high + low + close) / 3
    typical_sma_20 = typical_price.rolling(20).mean()
    typical_mad_20 = typical_price.rolling(20).apply(
        lambda values: float(np.mean(np.abs(values - np.mean(values)))),
        raw=True,
    )
    volume_sma_20 = volume.rolling(20).mean()

    features: dict[str, pd.Series] = {
        "return_1": returns,
        "log_return_1": np.log(safe_divide(close, prev_close)),
        "rolling_return_5": close.pct_change(5),
        "rolling_return_10": close.pct_change(10),
        "rolling_return_20": close.pct_change(20),
        "volatility_5": returns.rolling(5).std(),
        "volatility_10": returns.rolling(10).std(),
        "volatility_20": returns.rolling(20).std(),
        "sma_5": close.rolling(5).mean(),
        "sma_10": close.rolling(10).mean(),
        "sma_20": sma_20,
        "sma_50": close.rolling(50).mean(),
        "ema_12": ema_12,
        "ema_26": ema_26,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd - macd_signal,
        "rsi_14": 100 - (100 / (1 + rs)),
        "kdj_k": kdj_k,
        "kdj_d": kdj_d,
        "kdj_j": 3 * kdj_k - 2 * kdj_d,
        "atr_14": true_range.rolling(14).mean(),
        "boll_mid_20": sma_20,
        "boll_upper_20": boll_upper,
        "boll_lower_20": boll_lower,
        "boll_width_20": safe_divide(boll_upper - boll_lower, sma_20),
        "obv": (direction * volume).cumsum(),
        "volume_sma_20": volume_sma_20,
        "volume_ratio_20": safe_divide(volume, volume_sma_20),
        "hl_spread_pct": safe_divide(high - low, close),
        "close_position_in_range": safe_divide(close - low, high - low),
        "momentum_10": close - close.shift(10),
        "roc_10": 100 * close.pct_change(10),
        "williams_r_14": -100 * safe_divide(high_14 - close, high_14 - low_14),
        "cci_20": safe_divide(typical_price - typical_sma_20, 0.015 * typical_mad_20),
        "zscore_20": safe_divide(close - sma_20, std_20),
    }

    frames = []
    base = df[["Timestamp", "Ticker", "Interval"]].copy()
    for name, values in features.items():
        tmp = base.copy()
        tmp["Factor_Name"] = name
        tmp["Factor_Value"] = pd.to_numeric(values, errors="coerce")
        tmp = tmp.replace([math.inf, -math.inf], np.nan).dropna(subset=["Factor_Value"])
        frames.append(tmp)

    if not frames:
        return pd.DataFrame(columns=FACTOR_COLUMNS)
    return pd.concat(frames, ignore_index=True)[FACTOR_COLUMNS]


def calculate_and_store_features(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    interval: str,
) -> int:
    price_df = con.execute(
        """
        SELECT Timestamp, Ticker, Interval, Open, High, Low, Close, Volume
        FROM price_volume
        WHERE Ticker = ? AND Interval = ?
        ORDER BY Timestamp
        """,
        (ticker.upper(), interval),
    ).df()
    if price_df.empty:
        return 0

    register_factor_formulas(con)
    factor_df = compute_feature_frame(price_df)
    if factor_df.empty:
        return 0

    for factor_name in FACTOR_FORMULAS:
        con.execute(
            """
            DELETE FROM factor_values
            WHERE Ticker = ? AND Interval = ? AND Factor_Name = ?
            """,
            (ticker.upper(), interval, factor_name),
        )

    con.register("incoming_factors", factor_df)
    con.execute(
        """
        INSERT INTO factor_values
            (Timestamp, Ticker, Interval, Factor_Name, Factor_Value)
        SELECT Timestamp, Ticker, Interval, Factor_Name, Factor_Value
        FROM incoming_factors
        """
    )
    con.unregister("incoming_factors")
    return int(len(factor_df))
