"""
Ingest corporate actions (dividends + stock splits) for a symbol from yfinance.

Writes ASSET/<SYMBOL>/corporate_actions.parquet (chmod 444) and appends a manifest
entry. Idempotent: if the file already exists, refuses to overwrite — delete it first
to refresh (intentional, matches the immutability rule).
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
    out_path = out_dir / "corporate_actions.parquet"
    if out_path.exists():
        raise FileExistsError(
            f"{out_path} already exists — will not overwrite. "
            f"Delete it first to refresh."
        )

    t = yf.Ticker(symbol)
    actions = t.actions  # DataFrame: [Dividends, Stock Splits] indexed by Date
    if actions is None or actions.empty:
        print(f"[{symbol}] yfinance returned no actions")
        return

    # Flatten to long format
    rows: list[dict] = []
    for ts, row in actions.iterrows():
        ts_date = pd.Timestamp(ts).date()
        div = float(row.get("Dividends", 0) or 0)
        split = float(row.get("Stock Splits", 0) or 0)
        if div > 0:
            rows.append({
                "ts_date": ts_date,
                "symbol": symbol,
                "action_type": "dividend",
                "dividend_amount": div,
                "split_ratio": None,
            })
        if split and split != 1.0:
            rows.append({
                "ts_date": ts_date,
                "symbol": symbol,
                "action_type": "split",
                "dividend_amount": None,
                "split_ratio": split,
            })

    if not rows:
        print(f"[{symbol}] no dividends or splits to store")
        return

    df = pd.DataFrame(rows).sort_values("ts_date").reset_index(drop=True)
    df["source"] = "yfinance"
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    df.to_parquet(out_path, compression="zstd", index=False)
    os.chmod(out_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    entry = {
        "path": str(out_path.relative_to(DATA_ROOT)),
        "sha256": sha256_of_file(out_path),
        "rows": int(len(df)),
        "min_ts_date": str(df["ts_date"].min()),
        "max_ts_date": str(df["ts_date"].max()),
        "symbol": symbol,
        "kind": "corporate_actions",
        "source": "yfinance",
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    append_manifest(symbol, entry)

    n_div = int((df["action_type"] == "dividend").sum())
    n_split = int((df["action_type"] == "split").sum())
    print(
        f"[{symbol}] wrote {len(df)} rows "
        f"(dividends={n_div}, splits={n_split}) "
        f"range={entry['min_ts_date']}..{entry['max_ts_date']} "
        f"sha256={entry['sha256'][:12]}"
    )


if __name__ == "__main__":
    symbols = sys.argv[1:] or ["MSFT"]
    for s in symbols:
        try:
            ingest(s)
        except FileExistsError as e:
            print(f"[{s}] SKIP: {e}")
