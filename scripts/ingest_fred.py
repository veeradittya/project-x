"""
Ingest FRED (Federal Reserve Economic Data) series via the official FRED API.

Requires FRED_API_KEY in .env (free key: https://fred.stlouisfed.org/docs/api/api_key.html).

Each series → MACRO/<SERIES_ID>/series.parquet (chmod 444) + manifest entry.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DATA_ROOT = Path(os.getenv("DATA_ROOT") or ROOT)
MACRO_ROOT = DATA_ROOT / "MACRO"

API_BASE = "https://api.stlouisfed.org/fred/series/observations"

# series_id -> (frequency, description, units)
SERIES = {
    "DFF":      ("daily",   "Effective Federal Funds Rate",                    "percent"),
    "DGS10":    ("daily",   "10-Year Treasury Constant Maturity Rate",         "percent"),
    "DGS2":     ("daily",   "2-Year Treasury Constant Maturity Rate",          "percent"),
    "T10Y2Y":   ("daily",   "10Y–2Y Treasury Spread (inversion signal)",       "percent"),
    "VIXCLS":   ("daily",   "CBOE Volatility Index Close",                     "index"),
    "CPIAUCSL": ("monthly", "CPI All Urban Consumers, SA",                     "index_1982_84"),
    "UNRATE":   ("monthly", "US Unemployment Rate",                            "percent"),
}


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def append_manifest(series_id: str, entry: dict) -> None:
    path = MACRO_ROOT / series_id / "manifest.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def fetch_fred(series_id: str, api_key: str) -> pd.DataFrame:
    import subprocess
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": "1950-01-01",
    }
    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    r = subprocess.run(
        ["curl", "-sL", "--max-time", "30", url],
        capture_output=True, text=True, timeout=35,
    )
    if r.returncode != 0 or not r.stdout:
        raise RuntimeError(f"curl rc={r.returncode} stderr={r.stderr[:200]}")
    data = json.loads(r.stdout)

    obs = data.get("observations", [])
    if not obs:
        return pd.DataFrame()

    df = pd.DataFrame(obs)[["date", "value"]]
    df["ts_date"] = pd.to_datetime(df["date"]).dt.date
    # FRED uses "." for missing
    df["value"] = pd.to_numeric(df["value"].replace(".", None), errors="coerce")
    df = df.dropna(subset=["value"])[["ts_date", "value"]].reset_index(drop=True)
    return df


def ingest(series_id: str, api_key: str) -> None:
    freq, desc, units = SERIES.get(series_id, ("unknown", "", ""))
    out_dir = MACRO_ROOT / series_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "series.parquet"
    if out_path.exists():
        raise FileExistsError(f"{out_path} exists — delete to refresh.")

    df = fetch_fred(series_id, api_key)
    if df.empty:
        print(f"[{series_id}] no observations"); return

    df["series_id"]   = series_id
    df["frequency"]   = freq
    df["units"]       = units
    df["description"] = desc
    df["source"]      = "fred_api"
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")

    df = df[["ts_date", "series_id", "value", "frequency", "units",
             "description", "source", "ingested_at"]]
    df.to_parquet(out_path, compression="zstd", index=False)
    os.chmod(out_path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    entry = {
        "path": str(out_path.relative_to(DATA_ROOT)),
        "sha256": sha256_of_file(out_path),
        "rows": int(len(df)),
        "min_ts_date": str(df["ts_date"].min()),
        "max_ts_date": str(df["ts_date"].max()),
        "series_id": series_id,
        "frequency": freq,
        "units": units,
        "description": desc,
        "kind": "macro_series",
        "source": "fred_api",
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    append_manifest(series_id, entry)
    print(
        f"[{series_id}] {freq:>7}  {len(df):>6} obs  "
        f"{entry['min_ts_date']}..{entry['max_ts_date']} "
        f"latest={df.iloc[-1]['value']:.3f}  sha={entry['sha256'][:12]}  — {desc}"
    )


if __name__ == "__main__":
    key = os.environ.get("FRED_API_KEY")
    if not key:
        sys.exit("FRED_API_KEY missing from .env")
    sids = sys.argv[1:] or list(SERIES.keys())
    for sid in sids:
        try:
            ingest(sid, key)
        except FileExistsError as e:
            print(f"[{sid}] SKIP: {e}")
        except Exception as e:
            print(f"[{sid}] ERROR: {e}")
