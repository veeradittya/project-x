# Project X — Handoff

You are continuing a data engineering project from another Claude Code session.
Read this entire document before touching anything. It tells you:

1. What the project is and why it exists
2. The architecture and all the conventions that are already load-bearing
3. Everything that has been validated and is **not** up for debate
4. What the prior session left for you to do — the phased S&P 1500 ingest plan
5. The known pitfalls you should not rediscover the hard way

If something in the repo disagrees with this document, trust the code and update
the doc. The user works out of `/Users/veeradittya/Desktop/DATA/`.

**Data lives on an external SSD**, not in the repo. `ASSET/` and `MACRO/` are on
`/Volumes/Extreme SSD/project-x-data/` (APFS). Code paths resolve via the
`DATA_ROOT` env var in `.env` (defaults to the repo root if unset). See §2.

---

## 1. What this is

A **local, immutable market-data warehouse + Bloomberg-style web dashboard**,
built from scratch in Python (FastAPI + DuckDB + Alpaca SIP + yfinance + FRED +
FINRA Reg SHO) and vanilla JS (TradingView Lightweight Charts v4.2.2 bundled
locally). No cloud, no paid tier, no secrets in git.

The user's goal: a self-owned, auditable ledger of US equity price/volume plus
macro and corporate-event data, queryable through a polished single-user web UI.

**Current scope**: 8 symbols end-to-end (MSFT + SPY/QQQ/IWM/TLT/GLD/UUP/VIX).
**Next scope (this session)**: expand to the S&P 1500 — staged, budget-capped.

---

## 2. Architecture

### Split: code in repo, data on external SSD

```
/Users/veeradittya/Desktop/DATA/    # git repo (code + configs only)
├── .env                            # DATA_ROOT, Alpaca + FRED keys — NEVER commit
├── scripts/                        # (see below)
├── web/                            # (see below)
├── warehouse.duckdb                # regenerable query layer (delete to rebuild)
├── logs/                           # runtime logs from ingest_sp1500.py
└── HANDOFF.md / README.md / …

/Volumes/Extreme SSD/project-x-data/   # APFS — the data lives here
├── ASSET/                          # per-symbol immutable parquet store
│   └── <SYMBOL>/
│       ├── bars_1min/              # Hive-partitioned: year=YYYY/month=MM/bars.parquet
│       ├── bars_1d.parquet         # (ETFs/indexes only — daily yfinance)
│       ├── earnings.parquet
│       ├── corporate_actions.parquet
│       ├── short_volume.parquet
│       ├── metadata.json           # cached yfinance .info
│       ├── manifest.jsonl          # {path, sha256, rows, min_ts, max_ts, …} per file
│       └── verification_report.jsonl
└── MACRO/<SERIES_ID>/series.parquet  # FRED series (DFF, DGS10, CPIAUCSL, …)
```

Both code and server resolve the data location via the `DATA_ROOT` env var
(loaded from `.env` by every ingest script and by `web/server.py`). If
`DATA_ROOT` is empty, everything falls back to the repo root — so a fresh clone
without the SSD attached will still start up (it'll just see zero symbols).

```
scripts/
│   ├── ingest_alpaca.py            # 1-min bars from Alpaca SIP (free tier)
│   ├── ingest_daily_bars.py        # daily bars from yfinance (ETFs/indexes)
│   ├── ingest_earnings.py          # past + scheduled earnings from yfinance
│   ├── ingest_corporate_actions.py # dividends + splits from yfinance
│   ├── ingest_finra_short.py       # FINRA Reg SHO daily short-volume files
│   ├── ingest_fred.py              # FRED macro series
│   ├── ingest_sp1500.py            # 🆕 staged S&P 1500 driver (Phase 0)
│   ├── universes/
│   │   ├── fetch_sp1500.py         # 🆕 scrapes Wikipedia → per-tier CSVs
│   │   ├── sp500.csv  (503 rows)
│   │   ├── sp400.csv  (400 rows)
│   │   ├── sp600.csv  (603 rows)
│   │   └── sp1500.csv (1506 unique symbols, columns: symbol,name,sector,hq,tier,fetched_at)
│   ├── build_warehouse.py          # DuckDB views over ASSET/**/*.parquet
│   ├── verify.py                   # internal consistency, completeness, price, volume, manifest
│   ├── verify_volume_3source.py    # OURS (Alpaca SIP) vs YFINANCE vs FINRA Reg SHO
│   ├── verify_indicators.py
│   └── cross_verify_sources.py
├── web/
│   ├── server.py                   # FastAPI, DuckDB-in-memory view over ASSET/
│   └── static/
│       ├── index.html
│       ├── css/app.css
│       ├── js/app.js               # single-file frontend, no build step
│       └── vendor/lightweight-charts.standalone.production.js
```

### Invariants that must not be violated

These are not suggestions. Breaking them silently corrupts the ledger.

1. **Immutability of parquet files.** Every parquet file is `chmod 444` after
   write. Manifest records `sha256`, `rows`, `min_ts`, `max_ts`. The ingesters
   refuse to overwrite — you delete the file (+ its manifest line) to refresh.
2. **Regular-hours filter for 1-min bars.** `filter_regular_hours()` in
   `ingest_alpaca.py` keeps only `09:30 ≤ time < 16:00 ET` on NYSE trading
   days. Do not widen without the user's sign-off — it changes every historical
   aggregate downstream.
3. **Raw (unadjusted) prices.** Alpaca is pulled with `adjustment="raw"`. Splits
   and dividends live in `corporate_actions.parquet`; adjustment happens at
   query time (or in consumers) — never on the stored bars.
4. **UTC storage, ET bucketing for daily/weekly.** `ts_utc` is the canonical
   timestamp; server's `/api/assets/{sym}/bars` buckets in
   `America/New_York` for `1D`/`1W` so an April-17 09:30 ET bar maps to the
   April-17 date, not April-16 UTC.
5. **SIP volume ≠ consolidated volume.** Our Alpaca SIP volume runs ~20%
   below Yahoo/consolidated because it excludes off-exchange TRF prints.
   Verified against FINRA Reg SHO (3-source triangulation). See
   `verify_volume_3source.py`. **Do not "fix" this.**
6. **No secrets in git.** `.env` stays in `.gitignore`. If you need to run
   ingest scripts, expect the user to have the keys loaded locally.
7. **APFS-formatted `DATA_ROOT`.** The external SSD (`/Volumes/Extreme SSD`) is
   APFS — **not exFAT**. exFAT was tried and rejected: it ignores `chmod 444`
   (destroys Invariant 1), inflates small files ~9× due to 128 KB cluster
   sizing, and errors on `fchflags`. If the drive ever remounts as exFAT the
   warehouse is broken — halt and reformat before resuming ingest.
8. **Lightweight Charts v4.2.2**, not v5. v4 has no vertical-line primitive;
   earnings events are rendered via a DOM overlay (`evt-overlay` in CSS +
   `renderEarningsOverlay` in `app.js`). Do not "upgrade" LWC without talking
   to the user — there's working multi-pane sync and crosshair sync code
   that's coupled to v4 semantics.

### Data flow

```
Alpaca (SIP, free historical)     ──▶  ASSET/<SYM>/bars_1min/year=YYYY/month=MM/bars.parquet
yfinance                          ──▶  earnings / corp_actions / daily ETF bars / metadata
FINRA Reg SHO                     ──▶  short_volume
FRED                              ──▶  MACRO/<SERIES>/series.parquet
                                        │
                                        ▼
                               DuckDB (read_parquet + hive_partitioning)
                                        │
                                        ▼
                               FastAPI (web/server.py, port 8001)
                                        │
                                        ▼
                        JS dashboard (LWC multi-pane, earnings overlay)
```

Running locally:

```bash
# 1. Attach the Extreme SSD (APFS, mounts at /Volumes/Extreme SSD/).
# 2. Confirm .env has DATA_ROOT=/Volumes/Extreme SSD/project-x-data
# 3. Start the server:
source .venv/bin/activate
uvicorn web.server:app --host 127.0.0.1 --port 8001
# preview-managed; see .claude/launch.json
```

---

## 3. Validated and settled (do not reopen)

All of these were debated in earlier sessions and the decision stuck.

- **History window**: 6 years (2020-01-01 → present).
- **Universe**: S&P 1500 from Wikipedia, scraped via
  `scripts/universes/fetch_sp1500.py`. The committed CSVs *are* the audit
  trail — re-fetching overwrites them. Timestamp column `fetched_at` records
  when. Delisted-during-window symbols should be included when present in the
  snapshot; we're OK with Wikipedia's survivor-biased view for now.
- **Repo**: **public** on GitHub (`veeradittya/project-x`). The user chose
  this explicitly. Data (~22 MB for MSFT's 1-min bars) is committed.
- **Disk budget**: **< 21 GB** total for the full 1500. Budget is measured
  against `du -sh /Volumes/Extreme\ SSD/project-x-data/ASSET/` — the 931 GB
  SSD has plenty of headroom, but the 21 GB ceiling is a discipline signal,
  not a capacity concern.
- **Storage location**: external SSD (`Extreme SSD`, APFS). Reformatted from
  exFAT in this session — see Invariant 7.
- **Staging**: sequential. Each stage checkpoints before the next runs.
- **Volume discrepancy vs Yahoo**: accepted as normal. Documented in Invariant 5.
- **Earnings surprise**: displayed in **dollars**, not percent (user preference).
- **Timezone default**: UTC in the chart axis; HUD shows both UTC and ET.
- **DOM-overlay pattern for chart annotations**: LWC v4 lacks primitives, so
  vertical lines and event badges are a DOM layer positioned via
  `timeToCoordinate`. Updates re-run on `subscribeVisibleTimeRangeChange`.

---

## 4. Phased plan — **START HERE**

Phase 0 (universe + driver + sidebar) is **done**. You are starting at Stage 1.

### Stage 1 — S&P 500 (~11 GB, ~6 hrs wall clock)

```bash
source .venv/bin/activate
python scripts/ingest_sp1500.py --tier sp500
```

`ingest_sp1500.py` is resume-safe (per-month parquet immutability). Token-bucket
rate-limits at 180 req/min (Alpaca free tier allows 200 — we leave headroom).
Per-symbol errors are caught; progress rows go to `logs/sp1500_progress.jsonl`,
errors to `logs/sp1500_errors.jsonl`.

Then run earnings and corp actions for the same tier:

```bash
python -c "
import pandas as pd, subprocess, time, sys
syms = pd.read_csv('scripts/universes/sp500.csv')['symbol'].tolist()
for s in syms:
    subprocess.run([sys.executable, 'scripts/ingest_earnings.py', s])
    subprocess.run([sys.executable, 'scripts/ingest_corporate_actions.py', s])
    time.sleep(1.0)  # yfinance is flaky under pressure
"
```

(yfinance doesn't publish a rate limit but rewards ~1 req/sec.)

**Checkpoint** (required before Stage 2):
- `du -sh "/Volumes/Extreme SSD/project-x-data/ASSET/"` ≤ 11.5 GB
- Sample-audit with `python scripts/verify_volume_3source.py SYM …` on 10 random S&P 500 symbols — close diffs ≤ $0.20 median, volume ratio 70–95%
- Spot-check the dashboard sidebar: `S&P 500: ~503/503`

### Stage 2 — S&P MidCap 400 (~5 GB, ~4 hrs)

```bash
python scripts/ingest_sp1500.py --tier sp400
```

Same earnings/actions loop with `sp400.csv`. Checkpoint as above, cumulative
disk ≤ ~16.5 GB.

### Stage 3 — S&P SmallCap 600 (~4 GB, ~4 hrs)

```bash
python scripts/ingest_sp1500.py --tier sp600
```

**Budget decision at this gate**: if `du -sh "/Volumes/Extreme SSD/project-x-data/ASSET/"` after Stage 2 is > 16.5 GB,
SmallCap 600 1-min bars may push over the 21 GB cap. Fallback: do daily-only
for the SmallCap tier. You'll need to either:
- Skip the Alpaca 1-min call and write daily bars from yfinance per symbol, or
- Add a `--daily-only` flag to `ingest_sp1500.py` that bypasses
  `backfill()` and calls `ingest_daily_bars.py` equivalent.

Talk to the user before picking the fallback — they may prefer to accept a
slightly higher disk budget and keep 1-min everywhere.

### Stage 4 — full sweep verification

After all three tiers:

1. `python scripts/build_warehouse.py` — regenerates `warehouse.duckdb`
2. Random 30-symbol audit with `verify_volume_3source.py`
3. `du -sh "/Volumes/Extreme SSD/project-x-data/ASSET/"` — should be ≤ 21 GB
4. Load the dashboard; all three progress bars should read ≥ 95% done
5. Update `HANDOFF.md` with the actual final size, error count, and any
   delisted/missing tickers

---

## 5. Pitfalls — things that already burned prior sessions

- **Do not commit `.env`.** Alpaca + FRED API keys and `DATA_ROOT`. Already in `.gitignore`.
- **Confirm SSD is mounted before any ingest.** If `/Volumes/Extreme SSD/`
  isn't there, scripts will silently fall back to the repo path and pollute
  the working copy. Add `ls "/Volumes/Extreme SSD/project-x-data/ASSET" || exit 1`
  to any long-running shell.
- **Do not re-ingest an existing month.** `ingest_alpaca.write_month_parquet`
  raises `FileExistsError`. To refresh: `chmod u+w <path>` then `rm`, plus
  delete the matching manifest line. This is intentional friction.
- **yfinance rate limiting is real and silent.** If you see `earnings.parquet`
  for many symbols missing, you're hitting it. Throttle to 1 req/s.
- **Wikipedia 403s default `urlopen`.** `fetch_sp1500.py` sets a Chrome UA.
- **`pd.read_html` needs a valid parser.** `lxml` is in the venv.
- **Alpaca's SIP 15-min delay.** `ingest_alpaca.fetch_month` clips `end` to
  `now - 20 min`. Don't remove this — you'll get a 400 from Alpaca otherwise.
- **Wikipedia symbols use dots (`BRK.B`).** Alpaca accepts both dotted and
  dashed forms. We pass the Wikipedia form unchanged. Some tickers
  (`BF.B`, `BRK.B`, `MOG.A`) may still fail — if so, log and skip.
- **Chart multi-pane sync uses `subscribeVisibleTimeRangeChange`, not
  logical-range.** Don't "refactor" this; panes have different bar counts
  after indicator warmup and logical-range desyncs them.
- **DuckDB connections aren't thread-safe for concurrent `.execute()`.**
  `web/server.py` uses a process-wide `_base_con` and returns `.cursor()` per
  request. Keep that pattern.
- **`webkit` on macOS blocks `file://`.** The dashboard must be served over
  HTTP — use the preview server or uvicorn directly.
- **`preview_start` requires `.claude/launch.json`.** Already wired for
  `name: "dashboard"`.

---

## 6. Handy commands

```bash
# (Re)fetch the S&P 1500 universe
python scripts/universes/fetch_sp1500.py

# Dry run: 5 symbols of S&P 500, from 2024-01, to prove the pipeline
python scripts/ingest_sp1500.py --tier sp500 --limit 5 --start-year 2024 --start-month 1

# Full Stage 1
python scripts/ingest_sp1500.py --tier sp500

# Web preview
# preview_start with name="dashboard"  (runs `uvicorn web.server:app --port 8001`)

# Rebuild DuckDB
python scripts/build_warehouse.py

# 3-source volume check
python scripts/verify_volume_3source.py MSFT
```

---

## 7. Open questions the user hasn't answered

- **Delisted / corporate-action tickers during 6-year window**: Wikipedia
  snapshots are survivor-biased. A later initiative could add a historical-
  constituent source (e.g. Siblis, QuantConnect) to get the real 1500 for
  each month. Not a blocker — flag when you notice it matters for backtesting
  conversations.
- **Warehouse.duckdb in git**: committed for now (~275 KB). If it grows
  unwieldy after full ingest, move to `.gitignore` and rely on
  `build_warehouse.py` to regenerate.

Good luck. The foundation is solid; the heavy lifting is network IO, not
thinking. Checkpoint between stages so the user can audit before proceeding.
