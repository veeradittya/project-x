"""
Staged S&P 1500 ingester. Drives `ingest_alpaca.backfill` across a tier
(sp500 | sp400 | sp600 | all) of the universe defined in
scripts/universes/sp1500.csv.

Design notes
------------
* **Resume-safe**: each month's parquet is immutable, so re-running simply skips
  what's already there. Interrupt and restart with no state-file bookkeeping.
* **Rate-limited**: one shared RateLimiter at 180 req/min (Alpaca free tier
  ceiling is 200).
* **Failure isolation**: one symbol's errors never stop the tier. Per-symbol
  summaries and per-month errors are appended to `logs/sp1500_progress.jsonl`
  and `logs/sp1500_errors.jsonl`.
* **Ticker normalization**: Wikipedia sometimes uses dotted tickers (e.g.
  `BRK.B`, `BF.B`). Alpaca accepts both `BRK.B` and `BRK-B` — we pass the
  canonical dotted form and let Alpaca normalize. We do NOT create directory
  names that cross filesystem boundaries; the dot is filesystem-safe on APFS.

Usage
-----
    # Stage 1: S&P 500
    python scripts/ingest_sp1500.py --tier sp500

    # Stage 2: MidCap 400
    python scripts/ingest_sp1500.py --tier sp400

    # Stage 3: SmallCap 600
    python scripts/ingest_sp1500.py --tier sp600

    # Limit for testing
    python scripts/ingest_sp1500.py --tier sp500 --limit 5

    # Start from a specific year/month (default: 2020-01)
    python scripts/ingest_sp1500.py --tier sp500 --start-year 2020 --start-month 1
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from ingest_alpaca import RateLimiter, backfill, make_client  # noqa: E402

ROOT = HERE.parent
UNIVERSE_CSV = HERE / "universes" / "sp1500.csv"
LOG_DIR = ROOT / "logs"
PROGRESS_LOG = LOG_DIR / "sp1500_progress.jsonl"
ERROR_LOG = LOG_DIR / "sp1500_errors.jsonl"


def load_universe(tier: str) -> pd.DataFrame:
    if not UNIVERSE_CSV.exists():
        raise FileNotFoundError(
            f"{UNIVERSE_CSV} not found. Run "
            f"`python scripts/universes/fetch_sp1500.py` first."
        )
    df = pd.read_csv(UNIVERSE_CSV)
    if tier != "all":
        df = df[df["tier"] == tier]
    return df.reset_index(drop=True)


def log_jsonl(path: Path, entry: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def run(tier: str, start_year: int, start_month: int, limit: int | None, rpm: int) -> None:
    universe = load_universe(tier)
    if limit:
        universe = universe.head(limit)
    print(f"[driver] tier={tier} symbols={len(universe)} start={start_year}-{start_month:02d} rpm={rpm}")

    client = make_client()
    limiter = RateLimiter(rpm=rpm)
    t_start = time.time()

    for i, row in universe.iterrows():
        symbol = str(row["symbol"]).strip()
        t0 = time.time()
        print(f"\n[driver {i + 1}/{len(universe)}] {symbol} ({row.get('name', '')[:40]})")
        try:
            summary = backfill(
                symbol=symbol,
                start_year=start_year,
                start_month=start_month,
                client=client,
                limiter=limiter,
                log=True,
            )
        except Exception as e:
            # Unexpected — backfill normally catches per-month errors. Log and move on.
            err = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol,
                "tier": tier,
                "error": f"unhandled: {e}",
            }
            log_jsonl(ERROR_LOG, err)
            print(f"  [driver] UNHANDLED ERROR for {symbol}: {e}")
            continue

        elapsed = time.time() - t0
        summary["elapsed_s"] = round(elapsed, 2)
        summary["tier"] = tier
        summary["ts"] = datetime.now(timezone.utc).isoformat()
        log_jsonl(PROGRESS_LOG, summary)

        for y, m, msg in summary["errors"]:
            log_jsonl(
                ERROR_LOG,
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "symbol": symbol,
                    "tier": tier,
                    "year": y,
                    "month": m,
                    "error": msg,
                },
            )

        print(
            f"  [driver {symbol}] wrote={summary['months_written']} "
            f"skipped={summary['months_skipped']} empty={summary['months_empty']} "
            f"errored={summary['months_errored']} "
            f"rows={summary['rows_total']} ({elapsed:.1f}s)"
        )

    total_elapsed = time.time() - t_start
    print(f"\n[driver] tier={tier} DONE in {total_elapsed / 60:.1f} min")


def main() -> None:
    p = argparse.ArgumentParser(description="Stage-wise S&P 1500 ingester")
    p.add_argument("--tier", choices=["sp500", "sp400", "sp600", "all"], required=True)
    p.add_argument("--start-year", type=int, default=2020)
    p.add_argument("--start-month", type=int, default=1)
    p.add_argument("--limit", type=int, default=None, help="Only the first N symbols")
    p.add_argument("--rpm", type=int, default=180, help="Requests-per-minute cap")
    args = p.parse_args()

    run(
        tier=args.tier,
        start_year=args.start_year,
        start_month=args.start_month,
        limit=args.limit,
        rpm=args.rpm,
    )


if __name__ == "__main__":
    main()
