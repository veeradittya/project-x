"""
Immutable ingester for 1-min OHLCV bars from Alpaca (IEX feed).

For each (symbol, year, month):
  1. Skip if the month Parquet file already exists (idempotent — never overwrites).
  2. Pull all 1-min bars for that month from Alpaca.
  3. Filter to regular US equity trading hours (09:30-16:00 ET).
  4. Write Parquet to ASSET/<SYMBOL>/bars_1min/year=YYYY/month=MM/bars.parquet.
  5. chmod 444 (read-only at OS level).
  6. Append {path, sha256, rows, min_ts, max_ts, source, written_at} to manifest.jsonl.

Re-runs safely: existing months are never re-pulled or modified. To refresh a month,
you must manually delete its file and its manifest line — by design.
"""
from __future__ import annotations

import collections
import hashlib
import json
import os
import stat
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
# DATA_ROOT points at the immutable warehouse root (ASSET/, MACRO/). Defaults
# to the repo root; set DATA_ROOT=/Volumes/Extreme SSD/project-x-data in .env
# to route writes to the external SSD.
DATA_ROOT = Path(os.getenv("DATA_ROOT") or ROOT)
ASSET_ROOT = DATA_ROOT / "ASSET"
ET = "America/New_York"

# Alpaca free tier: 200 req/min. We stay at 180 for headroom.
DEFAULT_RPM = 180


class RateLimiter:
    """Thread-safe sliding-window request limiter: at most `rpm` calls per 60s."""

    def __init__(self, rpm: int = DEFAULT_RPM):
        self.rpm = rpm
        self._lock = threading.Lock()
        self._hits: collections.deque[float] = collections.deque()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                # Drop entries older than 60s
                while self._hits and (now - self._hits[0]) > 60.0:
                    self._hits.popleft()
                if len(self._hits) < self.rpm:
                    self._hits.append(now)
                    return
                sleep_for = 60.0 - (now - self._hits[0]) + 0.01
            if sleep_for > 0:
                time.sleep(sleep_for)


def make_client() -> StockHistoricalDataClient:
    key = os.environ["ALPACA_API_KEY_ID"]
    secret = os.environ["ALPACA_API_SECRET_KEY"]
    return StockHistoricalDataClient(key, secret)


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def month_iter(start: datetime, end: datetime):
    """Yield (year, month) from start to end inclusive."""
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def fetch_month(
    client: StockHistoricalDataClient,
    symbol: str,
    year: int,
    month: int,
    limiter: RateLimiter | None = None,
) -> pd.DataFrame:
    """Pull all 1-min bars for a calendar month. Returns DataFrame with UTC timestamp index."""
    from datetime import timedelta
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    # Alpaca's free tier requires SIP queries to be older than ~15 min.
    # Clip end to (now - 20 min) to stay safely in the historical window.
    now = datetime.now(timezone.utc) - timedelta(minutes=20)
    if end > now:
        end = now
    if start >= end:
        return pd.DataFrame()

    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=start,
        end=end,
        feed=DataFeed.SIP,  # consolidated tape — historical SIP is free on Alpaca
        adjustment="raw",   # raw prices, no split/div adjustment — we adjust at query time if needed
    )
    if limiter is not None:
        limiter.acquire()
    bars = client.get_stock_bars(req).df
    if bars.empty:
        return bars
    # Drop the symbol level from the MultiIndex -> flat timestamp index (UTC)
    bars = bars.reset_index()
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
    return bars


def filter_regular_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only bars whose timestamp falls within 09:30-16:00 ET on NYSE trading days."""
    if df.empty:
        return df
    ts_et = df["timestamp"].dt.tz_convert(ET)
    # 09:30 <= time < 16:00 ET
    in_hours = (ts_et.dt.time >= pd.Timestamp("09:30").time()) & (
        ts_et.dt.time < pd.Timestamp("16:00").time()
    )
    # NYSE trading day
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(
        start_date=ts_et.min().date(), end_date=ts_et.max().date()
    )
    trading_days = set(schedule.index.date)
    is_trading_day = ts_et.dt.date.isin(trading_days)
    return df[in_hours & is_trading_day].reset_index(drop=True)


def write_month_parquet(
    df: pd.DataFrame,
    symbol: str,
    year: int,
    month: int,
    source: str,
) -> dict | None:
    """Write the month's bars to an immutable Parquet file. Returns the manifest entry, or None if empty."""
    if df.empty:
        return None

    out_dir = ASSET_ROOT / symbol / "bars_1min" / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "bars.parquet"

    if out_path.exists():
        raise FileExistsError(f"{out_path} already exists — will not overwrite (immutability).")

    # Final schema
    out = pd.DataFrame({
        "ts_utc": df["timestamp"],
        "symbol": symbol,
        "open": df["open"].astype("float64"),
        "high": df["high"].astype("float64"),
        "low": df["low"].astype("float64"),
        "close": df["close"].astype("float64"),
        "volume": df["volume"].astype("int64"),
        "trade_count": df["trade_count"].astype("int32"),
        "vwap": df["vwap"].astype("float64"),
        "source": source,
        "ingested_at": pd.Timestamp.now(tz="UTC"),
    }).sort_values("ts_utc").reset_index(drop=True)

    out.to_parquet(out_path, compression="zstd", index=False)

    # Read-only at the OS level: owner read, group read, other read -> 0o444
    os.chmod(out_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    entry = {
        "path": str(out_path.relative_to(ROOT)),
        "sha256": sha256_of_file(out_path),
        "rows": int(len(out)),
        "min_ts_utc": out["ts_utc"].min().isoformat(),
        "max_ts_utc": out["ts_utc"].max().isoformat(),
        "symbol": symbol,
        "year": year,
        "month": month,
        "source": source,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    return entry


def append_manifest(symbol: str, entry: dict) -> None:
    path = ASSET_ROOT / symbol / "manifest.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def backfill(
    symbol: str,
    start_year: int,
    start_month: int,
    client: StockHistoricalDataClient | None = None,
    limiter: RateLimiter | None = None,
    log: bool = True,
) -> dict:
    """Backfill one symbol month-by-month. Resume-safe: existing month files are skipped.

    Returns a summary dict with month counts, time range, and collected errors.
    Never raises for per-month errors — they are recorded and collection continues.
    """
    if client is None:
        client = make_client()

    start = datetime(start_year, start_month, 1, tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    summary = {
        "symbol": symbol,
        "months_written": 0,
        "months_skipped": 0,
        "months_empty": 0,
        "months_errored": 0,
        "errors": [],
        "first_ts": None,
        "last_ts": None,
        "rows_total": 0,
    }

    for y, m in month_iter(start, now):
        out_path = (
            ASSET_ROOT / symbol / "bars_1min" / f"year={y}" / f"month={m:02d}" / "bars.parquet"
        )
        if out_path.exists():
            summary["months_skipped"] += 1
            if log:
                print(f"[{symbol} {y}-{m:02d}] exists, skipping")
            continue

        t0 = time.time()
        if log:
            print(f"[{symbol} {y}-{m:02d}] fetching…", end=" ", flush=True)
        try:
            raw = fetch_month(client, symbol, y, m, limiter=limiter)
        except Exception as e:
            summary["months_errored"] += 1
            summary["errors"].append((y, m, str(e)))
            if log:
                print(f"ERROR: {e}")
            continue

        if raw.empty:
            summary["months_empty"] += 1
            if log:
                print("no data returned (possibly pre-listing or future month)")
            continue

        filtered = filter_regular_hours(raw)
        if filtered.empty:
            summary["months_empty"] += 1
            if log:
                print(f"raw={len(raw)} -> 0 after regular-hours filter (all pre-market/after-hours?)")
            continue

        try:
            entry = write_month_parquet(filtered, symbol, y, m, source="alpaca_sip")
        except FileExistsError as e:
            summary["months_skipped"] += 1
            if log:
                print(f"RACE/EXISTS: {e}")
            continue

        if entry:
            append_manifest(symbol, entry)
            summary["months_written"] += 1
            summary["rows_total"] += entry["rows"]
            if summary["first_ts"] is None or entry["min_ts_utc"] < summary["first_ts"]:
                summary["first_ts"] = entry["min_ts_utc"]
            if summary["last_ts"] is None or entry["max_ts_utc"] > summary["last_ts"]:
                summary["last_ts"] = entry["max_ts_utc"]
            if log:
                dt = time.time() - t0
                print(
                    f"raw={len(raw)} kept={entry['rows']} "
                    f"range={entry['min_ts_utc']}..{entry['max_ts_utc']} "
                    f"sha256={entry['sha256'][:12]} ({dt:.1f}s)"
                )

    return summary


if __name__ == "__main__":
    symbol = sys.argv[1] if len(sys.argv) > 1 else "MSFT"
    start_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2020
    start_month = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    backfill(symbol, start_year, start_month, limiter=RateLimiter())
