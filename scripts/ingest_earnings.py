"""
Ingest earnings history for a symbol.

Primary source: yfinance.Ticker.earnings_dates — returns a mix of past (with
Reported EPS) and upcoming (Reported EPS is NaN, just the scheduled date).

We keep BOTH: past rows carry actuals + estimates + surprise; future rows carry
the scheduled date (and estimate if published). A `status` column distinguishes
them: "reported" vs "scheduled".
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


def ingest(symbol: str) -> None:
    out_dir = ASSET_ROOT / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "earnings.parquet"
    if out_path.exists():
        raise FileExistsError(f"{out_path} exists — delete to refresh.")

    t = yf.Ticker(symbol)
    # Pull a wide window: past (reported) + future (scheduled).
    # yfinance's get_earnings_dates caps at ~80 rows; this covers ~20 years.
    df = t.get_earnings_dates(limit=80)
    if df is None or df.empty:
        print(f"[{symbol}] no earnings dates from yfinance")
        return

    df = df.reset_index().rename(columns={
        "Earnings Date": "earnings_ts",
        "EPS Estimate": "eps_estimate",
        "Reported EPS": "eps_actual",
        "Surprise(%)": "eps_surprise_pct",
    })
    df["earnings_ts"] = pd.to_datetime(df["earnings_ts"], utc=True)

    # Classify past (has actuals) vs scheduled future (missing actuals)
    df["status"] = df["eps_actual"].notna().map({True: "reported", False: "scheduled"})

    df["symbol"] = symbol
    df["source"] = "yfinance"
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")
    keep = ["earnings_ts", "symbol", "eps_actual", "eps_estimate",
            "eps_surprise_pct", "status", "source", "ingested_at"]
    df = df[[c for c in keep if c in df.columns]]
    df = df.sort_values("earnings_ts").reset_index(drop=True)

    df.to_parquet(out_path, compression="zstd", index=False)
    os.chmod(out_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    entry = {
        "path": str(out_path.relative_to(DATA_ROOT)),
        "sha256": sha256_of_file(out_path),
        "rows": int(len(df)),
        "min_earnings_ts": df["earnings_ts"].min().isoformat(),
        "max_earnings_ts": df["earnings_ts"].max().isoformat(),
        "symbol": symbol,
        "kind": "earnings",
        "source": "yfinance",
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    append_manifest(symbol, entry)
    beats = int((df["eps_surprise_pct"] > 0).sum())
    misses = int((df["eps_surprise_pct"] < 0).sum())
    scheduled = int((df["status"] == "scheduled").sum())
    print(
        f"[{symbol}] wrote {len(df)} earnings "
        f"reported={len(df)-scheduled} scheduled={scheduled} "
        f"beats={beats} misses={misses} "
        f"range={entry['min_earnings_ts']}..{entry['max_earnings_ts']} "
        f"sha256={entry['sha256'][:12]}"
    )


if __name__ == "__main__":
    for s in (sys.argv[1:] or ["MSFT"]):
        try:
            ingest(s)
        except FileExistsError as e:
            print(f"[{s}] SKIP: {e}")
