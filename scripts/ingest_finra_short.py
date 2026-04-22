"""
Ingest FINRA daily short-sale volume (Reg SHO) for a symbol.

Endpoint: https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt
Format:   pipe-delimited, columns: Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market

No auth required. One file per trading day. We iterate trading days (NYSE calendar),
fetch each file, extract our symbol's row, aggregate to ASSET/<SYMBOL>/short_volume.parquet.
"""
from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import os
import stat
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DATA_ROOT = Path(os.getenv("DATA_ROOT") or ROOT)
ASSET_ROOT = DATA_ROOT / "ASSET"

URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{d}.txt"


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


def fetch_day(d: date, symbol: str) -> dict | None:
    """Fetch one day's FINRA CSV and extract the target symbol's row. Returns None if absent/error."""
    url = URL.format(d=d.strftime("%Y%m%d"))
    try:
        r = subprocess.run(
            ["curl", "-sL", "--max-time", "20", url],
            capture_output=True, text=True, timeout=25,
        )
        if r.returncode != 0 or not r.stdout:
            return None
    except Exception:
        return None

    sym = symbol.upper()
    for line in r.stdout.splitlines():
        parts = line.split("|")
        if len(parts) >= 5 and parts[1] == sym:
            try:
                return {
                    "ts_date":               d,
                    "symbol":                sym,
                    "short_volume":          float(parts[2]),
                    "short_exempt_volume":   float(parts[3]),
                    "total_volume":          float(parts[4]),
                    "market":                parts[5] if len(parts) > 5 else "",
                }
            except ValueError:
                return None
    return None


def ingest(symbol: str, start: str = "2020-01-02") -> None:
    out_dir = ASSET_ROOT / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "short_volume.parquet"
    if out_path.exists():
        raise FileExistsError(f"{out_path} exists — delete to refresh.")

    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=start, end_date=date.today().isoformat())
    days: list[date] = [d.date() for d in sched.index]
    print(f"[{symbol}] {len(days)} trading days to fetch...")

    rows: list[dict] = []
    missing = 0
    with cf.ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(fetch_day, d, symbol): d for d in days}
        done = 0
        for fut in cf.as_completed(futures):
            row = fut.result()
            if row:
                rows.append(row)
            else:
                missing += 1
            done += 1
            if done % 200 == 0:
                print(f"  ... {done}/{len(days)} fetched, {missing} missing")

    if not rows:
        print(f"[{symbol}] nothing collected")
        return

    df = pd.DataFrame(rows).sort_values("ts_date").reset_index(drop=True)
    df["short_pct"]   = (df["short_volume"] / df["total_volume"] * 100).round(3)
    df["source"]      = "finra_regsho"
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    df = df[["ts_date", "symbol", "short_volume", "short_exempt_volume",
             "total_volume", "short_pct", "market", "source", "ingested_at"]]
    df.to_parquet(out_path, compression="zstd", index=False)
    os.chmod(out_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    entry = {
        "path": str(out_path.relative_to(DATA_ROOT)),
        "sha256": sha256_of_file(out_path),
        "rows": int(len(df)),
        "min_ts_date": str(df["ts_date"].min()),
        "max_ts_date": str(df["ts_date"].max()),
        "symbol": symbol,
        "kind": "short_volume",
        "source": "finra_regsho",
        "missing_days": missing,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    append_manifest(symbol, entry)

    avg_pct = df["short_pct"].mean()
    latest = df.iloc[-1]
    print(
        f"[{symbol}] wrote {len(df)} days (missing={missing}) "
        f"range={entry['min_ts_date']}..{entry['max_ts_date']} "
        f"avg short%={avg_pct:.1f}  latest({latest['ts_date']})={latest['short_pct']:.1f}%  "
        f"sha={entry['sha256'][:12]}"
    )


if __name__ == "__main__":
    for s in (sys.argv[1:] or ["MSFT"]):
        try:
            ingest(s)
        except FileExistsError as e:
            print(f"[{s}] SKIP: {e}")
