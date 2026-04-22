"""
Cross-check our technical indicator computations against:
  1. Naive hand-rolled formulas (the textbook reference)
  2. pandas-ta  (if installed) — widely used TA library
  3. ta          (if installed) — another widely used TA library
  4. TA-Lib      (if installed) — the C reference lib

Pulls MSFT daily closes/volumes from our DB via the /bars endpoint path and computes:
  SMA(20), EMA(12), VWAP (cumulative), Bollinger(20, 2σ)

Reports per-indicator max abs diff vs reference.
"""
from __future__ import annotations
import importlib
import math
import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DATA_ROOT = Path(os.getenv("DATA_ROOT") or ROOT)
ASSET_ROOT = DATA_ROOT / "ASSET"

# ---- pull daily OHLCV from DB, same logic as /api/assets/{sym}/bars?tf=1D ----
con = duckdb.connect()
df = con.sql(f"""
    WITH src AS (
        SELECT * FROM read_parquet(
            '{ASSET_ROOT}/MSFT/bars_1min/**/*.parquet',
            hive_partitioning=true)
    )
    SELECT
        CAST(ts_utc AT TIME ZONE 'America/New_York' AS DATE) AS day,
        arg_min(open, ts_utc)  AS open,
        MAX(high)              AS high,
        MIN(low)               AS low,
        arg_max(close, ts_utc) AS close,
        SUM(volume)            AS volume
    FROM src
    GROUP BY 1 ORDER BY 1
""").df()

close  = df["close"].astype(float)
volume = df["volume"].astype(float)
print(f"Daily bars: {len(df)}  range: {df['day'].min()} → {df['day'].max()}")


# ---- our backend's formulas (copy exactly) -----------------------------------
ours = {
    "sma20":  close.rolling(20, min_periods=20).mean(),
    "ema12":  close.ewm(span=12, adjust=False).mean(),
    "vwap":   (close * volume).cumsum() / volume.cumsum(),
}
_mid = close.rolling(20, min_periods=20).mean()
# Population std (ddof=0) — matches TA-Lib / TradingView / Bloomberg / original Bollinger spec
_std = close.rolling(20, min_periods=20).std(ddof=0)
ours["bb20_mid"]   = _mid
ours["bb20_upper"] = _mid + 2 * _std
ours["bb20_lower"] = _mid - 2 * _std


# ---- reference #1: naive hand-rolled --------------------------------------
def sma_manual(x: pd.Series, n: int) -> pd.Series:
    out = pd.Series([np.nan] * len(x), index=x.index)
    for i in range(n - 1, len(x)):
        out.iloc[i] = x.iloc[i - n + 1: i + 1].mean()
    return out

def ema_manual(x: pd.Series, span: int) -> pd.Series:
    """Standard EMA with alpha = 2 / (span+1), seeded by SMA of first `span` values."""
    alpha = 2.0 / (span + 1)
    out = pd.Series([np.nan] * len(x), index=x.index)
    # Seed with simple mean of first `span` values (common convention; pandas adjust=False seeds with x[0])
    # pandas adjust=False definition: EMA[0] = x[0]; EMA[t] = alpha*x[t] + (1-alpha)*EMA[t-1]
    out.iloc[0] = x.iloc[0]
    for i in range(1, len(x)):
        out.iloc[i] = alpha * x.iloc[i] + (1 - alpha) * out.iloc[i - 1]
    return out

def vwap_manual(p: pd.Series, v: pd.Series) -> pd.Series:
    pv = (p * v).cumsum()
    vv = v.cumsum()
    return pv / vv

def bollinger_manual(x: pd.Series, n: int, k: float, ddof: int) -> tuple[pd.Series, pd.Series, pd.Series]:
    mid = sma_manual(x, n)
    std = pd.Series([np.nan] * len(x), index=x.index)
    for i in range(n - 1, len(x)):
        std.iloc[i] = x.iloc[i - n + 1: i + 1].std(ddof=ddof)
    return mid, mid + k * std, mid - k * std

manual = {
    "sma20": sma_manual(close, 20),
    "ema12": ema_manual(close, 12),
    "vwap":  vwap_manual(close, volume),
}
mid, up, lo = bollinger_manual(close, 20, 2.0, ddof=0)  # matches TA-Lib convention
manual.update({"bb20_mid": mid, "bb20_upper": up, "bb20_lower": lo})


# ---- reference #2: pandas-ta ---------------------------------------------
try:
    pta = importlib.import_module("pandas_ta")
except Exception:
    pta = None

# ---- reference #3: talib --------------------------------------------------
try:
    talib = importlib.import_module("talib")
except Exception:
    talib = None


def diff_report(name: str, a: pd.Series, b: pd.Series) -> dict:
    m = (~a.isna()) & (~b.isna())
    if m.sum() == 0:
        return {"name": name, "n": 0}
    d = (a[m] - b[m]).abs()
    rel = d / a[m].abs().replace(0, np.nan)
    return {
        "name": name,
        "n": int(m.sum()),
        "max_abs": float(d.max()),
        "mean_abs": float(d.mean()),
        "max_rel": float(rel.max() if rel.notna().any() else 0),
    }


# Compare ours vs manual (same formulas) — should be ~0
print("\n=== OURS vs MANUAL (identical formulas — expect max_abs ~0) ===")
for k in ours:
    print(" ", diff_report(k, ours[k], manual[k]))

# pandas-ta
if pta is not None:
    print("\n=== OURS vs pandas-ta ===")
    pta_sma20 = pta.sma(close, length=20)
    pta_ema12 = pta.ema(close, length=12)
    pta_vwap  = None  # pandas-ta vwap needs H/L/C/V and is session-based; skip
    pta_bb    = pta.bbands(close, length=20, std=2)
    print(" ", diff_report("sma20",      ours["sma20"],      pta_sma20))
    print(" ", diff_report("ema12",      ours["ema12"],      pta_ema12))
    if pta_bb is not None:
        mid_col = [c for c in pta_bb.columns if c.startswith("BBM")][0]
        up_col  = [c for c in pta_bb.columns if c.startswith("BBU")][0]
        lo_col  = [c for c in pta_bb.columns if c.startswith("BBL")][0]
        print(" ", diff_report("bb20_mid",   ours["bb20_mid"],   pta_bb[mid_col]))
        print(" ", diff_report("bb20_upper", ours["bb20_upper"], pta_bb[up_col]))
        print(" ", diff_report("bb20_lower", ours["bb20_lower"], pta_bb[lo_col]))

# ta library
ta = None
try:
    ta = importlib.import_module("ta")
except Exception:
    pass
if ta is not None:
    print("\n=== OURS vs ta (from ta-library) ===")
    ta_sma20 = ta.trend.SMAIndicator(close, window=20, fillna=False).sma_indicator()
    ta_ema12 = ta.trend.EMAIndicator(close, window=12, fillna=False).ema_indicator()
    ta_bb    = ta.volatility.BollingerBands(close, window=20, window_dev=2, fillna=False)
    print(" ", diff_report("sma20",      ours["sma20"],      ta_sma20))
    print(" ", diff_report("ema12",      ours["ema12"],      ta_ema12))
    print(" ", diff_report("bb20_mid",   ours["bb20_mid"],   ta_bb.bollinger_mavg()))
    print(" ", diff_report("bb20_upper", ours["bb20_upper"], ta_bb.bollinger_hband()))
    print(" ", diff_report("bb20_lower", ours["bb20_lower"], ta_bb.bollinger_lband()))

# TA-Lib
if talib is not None:
    print("\n=== OURS vs TA-Lib ===")
    tl_sma20 = pd.Series(talib.SMA(close.values, timeperiod=20), index=close.index)
    tl_ema12 = pd.Series(talib.EMA(close.values, timeperiod=12), index=close.index)
    tl_u, tl_m, tl_l = talib.BBANDS(close.values, timeperiod=20, nbdevup=2, nbdevdn=2)
    tl_bb_u = pd.Series(tl_u, index=close.index)
    tl_bb_m = pd.Series(tl_m, index=close.index)
    tl_bb_l = pd.Series(tl_l, index=close.index)
    print(" ", diff_report("sma20",      ours["sma20"],      tl_sma20))
    print(" ", diff_report("ema12",      ours["ema12"],      tl_ema12))
    print(" ", diff_report("bb20_mid",   ours["bb20_mid"],   tl_bb_m))
    print(" ", diff_report("bb20_upper", ours["bb20_upper"], tl_bb_u))
    print(" ", diff_report("bb20_lower", ours["bb20_lower"], tl_bb_l))

# Spot check: print the last 5 rows of each indicator, side-by-side with manual
print("\n=== LAST 5 ROWS (ours vs manual) ===")
cmp = pd.DataFrame({
    "day":   df["day"].tail(5).values,
    "close": close.tail(5).values,
    "sma20_ours":   ours["sma20"].tail(5).values,
    "sma20_manual": manual["sma20"].tail(5).values,
    "ema12_ours":   ours["ema12"].tail(5).values,
    "ema12_manual": manual["ema12"].tail(5).values,
    "vwap_ours":    ours["vwap"].tail(5).values,
    "vwap_manual":  manual["vwap"].tail(5).values,
    "bbupper_ours": ours["bb20_upper"].tail(5).values,
    "bbupper_man":  manual["bb20_upper"].tail(5).values,
})
print(cmp.to_string(index=False))
