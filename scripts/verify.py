"""
Accuracy & integrity checks for stored MSFT 1-min bars.

A. Internal consistency — OHLC ordering, non-negative volume, sorted timestamps, no duplicates.
B. Completeness      — every NYSE trading day in range has bars.
C. Price accuracy    — daily aggregate OHLC from our 1-min bars vs yfinance daily OHLC.
D. Volume sanity     — daily IEX volume / yfinance consolidated volume should be ~1-5%.
E. Manifest integrity — re-hash every Parquet file, compare to manifest.jsonl.

Writes a JSON report to ASSET/<SYMBOL>/verification_report.jsonl (append-only).
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf

ROOT = Path(__file__).resolve().parent.parent
ASSET_ROOT = ROOT / "ASSET"
ET = "America/New_York"


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_bars(symbol: str) -> pd.DataFrame:
    glob = str(ASSET_ROOT / symbol / "bars_1min" / "**" / "*.parquet")
    con = duckdb.connect()
    df = con.sql(f"""
        SELECT ts_utc, open, high, low, close, volume, trade_count, vwap
        FROM read_parquet('{glob}', hive_partitioning=true)
        ORDER BY ts_utc
    """).df()
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    return df


# -------- A. internal consistency ------------------------------------------
def check_internal(df: pd.DataFrame) -> dict:
    issues = {}
    # OHLC ordering
    bad_high = ((df["high"] < df[["open", "close"]].max(axis=1)) | (df["high"] < df["low"])).sum()
    bad_low = ((df["low"] > df[["open", "close"]].min(axis=1)) | (df["low"] > df["high"])).sum()
    issues["bars_with_bad_high"] = int(bad_high)
    issues["bars_with_bad_low"] = int(bad_low)
    # Non-negative
    issues["bars_with_negative_volume"] = int((df["volume"] < 0).sum())
    issues["bars_with_nonpositive_price"] = int(
        ((df[["open", "high", "low", "close"]] <= 0).any(axis=1)).sum()
    )
    # Duplicates & ordering
    issues["duplicate_timestamps"] = int(df["ts_utc"].duplicated().sum())
    issues["out_of_order_rows"] = int((df["ts_utc"].diff().dt.total_seconds() < 0).sum())
    issues["total_bars"] = int(len(df))
    return issues


# -------- B. completeness --------------------------------------------------
def check_completeness(df: pd.DataFrame) -> dict:
    ts_et = df["ts_utc"].dt.tz_convert(ET)
    first_day = ts_et.min().date()
    last_day = ts_et.max().date()
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=first_day, end_date=last_day)
    expected_days = set(schedule.index.date)
    present_days = set(ts_et.dt.date.unique())
    missing = sorted(expected_days - present_days)
    extra = sorted(present_days - expected_days)
    return {
        "first_day": str(first_day),
        "last_day": str(last_day),
        "expected_trading_days": len(expected_days),
        "present_trading_days": len(present_days),
        "missing_days_count": len(missing),
        "missing_days_sample": [str(d) for d in missing[:10]],
        "unexpected_days_count": len(extra),
    }


# -------- C. price accuracy vs yfinance ------------------------------------
def check_price_accuracy(symbol: str, df: pd.DataFrame) -> dict:
    ts_et = df["ts_utc"].dt.tz_convert(ET)
    df = df.assign(day=ts_et.dt.date)

    ours = df.groupby("day").agg(
        our_open=("open", "first"),
        our_high=("high", "max"),
        our_low=("low", "min"),
        our_close=("close", "last"),
        our_volume=("volume", "sum"),
    ).reset_index()

    yf_df = yf.download(
        symbol,
        start=str(ours["day"].min()),
        end=str(ours["day"].max() + pd.Timedelta(days=1)),
        interval="1d",
        progress=False,
        auto_adjust=False,
    )
    # yfinance sometimes returns multiindex columns when group=False
    if isinstance(yf_df.columns, pd.MultiIndex):
        yf_df.columns = yf_df.columns.get_level_values(0)
    yf_df = yf_df.reset_index().rename(
        columns={"Date": "day", "Open": "yf_open", "High": "yf_high", "Low": "yf_low",
                 "Close": "yf_close", "Volume": "yf_volume"}
    )
    yf_df["day"] = pd.to_datetime(yf_df["day"]).dt.date

    m = ours.merge(yf_df[["day", "yf_open", "yf_high", "yf_low", "yf_close", "yf_volume"]],
                   on="day", how="inner")

    # Differences
    m["d_open"] = m["our_open"] - m["yf_open"]
    m["d_close"] = m["our_close"] - m["yf_close"]
    m["d_high"] = m["our_high"] - m["yf_high"]      # expect <= 0 (we clip)
    m["d_low"] = m["our_low"] - m["yf_low"]         # expect >= 0 (we clip)
    m["vol_ratio"] = m["our_volume"] / m["yf_volume"]

    large_close = m[m["d_close"].abs() > 1.0]  # > $1 divergence = anomaly
    report = {
        "days_compared": int(len(m)),
        "close_diff_mean": round(float(m["d_close"].mean()), 4),
        "close_diff_median": round(float(m["d_close"].median()), 4),
        "close_diff_abs_mean": round(float(m["d_close"].abs().mean()), 4),
        "close_diff_abs_p95": round(float(m["d_close"].abs().quantile(0.95)), 4),
        "close_diff_abs_max": round(float(m["d_close"].abs().max()), 4),
        "days_with_close_diff_gt_1usd": int(len(large_close)),
        "days_our_high_lt_yf_high_pct": round(float((m["d_high"] < 0).mean() * 100), 2),
        "days_our_low_gt_yf_low_pct": round(float((m["d_low"] > 0).mean() * 100), 2),
        "high_clip_mean_usd": round(float(m.loc[m["d_high"] < 0, "d_high"].mean()), 4) if (m["d_high"] < 0).any() else 0,
        "low_clip_mean_usd": round(float(m.loc[m["d_low"] > 0, "d_low"].mean()), 4) if (m["d_low"] > 0).any() else 0,
        "volume_ratio_mean_pct": round(float(m["vol_ratio"].mean() * 100), 3),
        "volume_ratio_median_pct": round(float(m["vol_ratio"].median() * 100), 3),
        "volume_ratio_min_pct": round(float(m["vol_ratio"].min() * 100), 3),
        "volume_ratio_max_pct": round(float(m["vol_ratio"].max() * 100), 3),
    }
    if not large_close.empty:
        report["large_close_diff_samples"] = (
            large_close[["day", "our_close", "yf_close", "d_close"]]
            .head(10).to_dict(orient="records")
        )
        # Convert dates for JSON
        for r in report["large_close_diff_samples"]:
            r["day"] = str(r["day"])
    return report


# -------- E. manifest integrity --------------------------------------------
def check_manifest(symbol: str) -> dict:
    man_path = ASSET_ROOT / symbol / "manifest.jsonl"
    if not man_path.exists():
        return {"manifest_exists": False}
    entries = [json.loads(l) for l in man_path.read_text().splitlines() if l.strip()]
    bad = []
    for e in entries:
        p = ROOT / e["path"]
        if not p.exists():
            bad.append({"path": e["path"], "issue": "missing_file"})
            continue
        actual = sha256_of_file(p)
        if actual != e["sha256"]:
            bad.append({"path": e["path"], "issue": "hash_mismatch",
                        "expected": e["sha256"], "actual": actual})
    return {
        "manifest_entries": len(entries),
        "corrupted_or_missing": len(bad),
        "issues_sample": bad[:10],
    }


def main(symbol: str) -> None:
    print(f"Loading bars for {symbol}...")
    df = load_bars(symbol)
    print(f"  {len(df):,} rows loaded")

    print("\nA. Internal consistency...")
    A = check_internal(df)
    for k, v in A.items():
        print(f"  {k}: {v}")

    print("\nB. Completeness...")
    B = check_completeness(df)
    for k, v in B.items():
        print(f"  {k}: {v}")

    print("\nC. Price accuracy vs yfinance (daily)...")
    C = check_price_accuracy(symbol, df)
    for k, v in C.items():
        if isinstance(v, list):
            print(f"  {k}:")
            for item in v:
                print(f"    {item}")
        else:
            print(f"  {k}: {v}")

    print("\nE. Manifest integrity...")
    E = check_manifest(symbol)
    for k, v in E.items():
        if isinstance(v, list):
            print(f"  {k}: {v[:3]}{' ...' if len(v) > 3 else ''}")
        else:
            print(f"  {k}: {v}")

    # Append full report
    report = {
        "symbol": symbol,
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "internal_consistency": A,
        "completeness": B,
        "price_accuracy": C,
        "manifest_integrity": E,
    }
    rpt_path = ASSET_ROOT / symbol / "verification_report.jsonl"
    with rpt_path.open("a") as f:
        f.write(json.dumps(report, default=str) + "\n")
    print(f"\nFull report appended to {rpt_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "MSFT")
