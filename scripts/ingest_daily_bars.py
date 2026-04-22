"""
Ingest daily OHLCV bars for index/ETF symbols from yfinance.

Writes ASSET/<SYMBOL>/bars_1d.parquet (chmod 444) and appends a manifest entry.

Special handling:
  * VIX is a non-tradable index — yfinance ticker is '^VIX'. We store it as symbol='VIX'
    for a clean namespace.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DATA_ROOT = Path(os.getenv("DATA_ROOT") or ROOT)
ASSET_ROOT = DATA_ROOT / "ASSET"

# Display symbol -> yfinance ticker
SYMBOL_MAP = {
    "SPY": "SPY",
    "QQQ": "QQQ",
    "IWM": "IWM",
    "TLT": "TLT",
    "GLD": "GLD",
    "UUP": "UUP",
    "VIX": "^VIX",
}


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def append_manifest(symbol: str, entry: dict) -> None:
    path = ASSET_ROOT / symbol / "manifest.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def ingest(display_symbol: str, start: str = "2015-01-01") -> None:
    yf_ticker = SYMBOL_MAP.get(display_symbol, display_symbol)
    out_dir = ASSET_ROOT / display_symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "bars_1d.parquet"
    if out_path.exists():
        raise FileExistsError(f"{out_path} exists — delete to refresh.")

    df = yf.download(
        yf_ticker, start=start, interval="1d", progress=False, auto_adjust=False,
    )
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index().rename(columns={
        "Date": "ts_date", "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
    })
    if df.empty:
        print(f"[{display_symbol}] no data from yfinance")
        return

    df["ts_date"] = pd.to_datetime(df["ts_date"]).dt.date
    df["symbol"] = display_symbol
    df["source"] = "yfinance"
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    keep = ["ts_date", "symbol", "open", "high", "low", "close", "adj_close",
            "volume", "source", "ingested_at"]
    df = df[[c for c in keep if c in df.columns]].sort_values("ts_date").reset_index(drop=True)
    df.to_parquet(out_path, compression="zstd", index=False)
    os.chmod(out_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    entry = {
        "path": str(out_path.relative_to(ROOT)),
        "sha256": sha256_of_file(out_path),
        "rows": int(len(df)),
        "min_ts_date": str(df["ts_date"].min()),
        "max_ts_date": str(df["ts_date"].max()),
        "symbol": display_symbol,
        "kind": "bars_1d",
        "source": "yfinance",
        "yfinance_ticker": yf_ticker,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    append_manifest(display_symbol, entry)
    print(
        f"[{display_symbol}] ({yf_ticker}) wrote {len(df)} bars "
        f"range={entry['min_ts_date']}..{entry['max_ts_date']} "
        f"sha256={entry['sha256'][:12]}"
    )


if __name__ == "__main__":
    syms = sys.argv[1:] or list(SYMBOL_MAP.keys())
    for s in syms:
        try:
            ingest(s)
        except FileExistsError as e:
            print(f"[{s}] SKIP: {e}")
