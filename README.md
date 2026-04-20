# Project X

Local, immutable market-data warehouse + Bloomberg-style web dashboard for US
equities. Everything runs on one laptop, no cloud, no paid API tier.

## Stack

- **Ingest**: Alpaca SIP (1-min bars), yfinance (earnings, corp actions, daily
  ETFs, metadata), FINRA Reg SHO (short volume), FRED (macro series)
- **Storage**: immutable Parquet + SHA256 manifests under `ASSET/<SYM>/` and
  `MACRO/<SERIES>/`
- **Query**: DuckDB reading Parquet directly (no ETL into the DB)
- **Web**: FastAPI + vanilla JS + TradingView Lightweight Charts v4.2.2
  (bundled locally)

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt  # see imports in scripts/*.py if requirements.txt is absent
cp .env.example .env              # fill in ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY, FRED_API_KEY
uvicorn web.server:app --host 127.0.0.1 --port 8001
open http://127.0.0.1:8001
```

## Status

- MSFT is fully ingested (6 years of 1-min bars + events + short volume + earnings)
- 7 ETFs/indexes ingested at daily resolution (SPY, QQQ, IWM, TLT, GLD, UUP, VIX)
- S&P 1500 universe fetched (503 + 400 + 603 = 1506 unique symbols) — see
  `scripts/universes/sp1500.csv`
- Staged S&P 1500 ingester scaffolded: `scripts/ingest_sp1500.py`

## Next

See [HANDOFF.md](HANDOFF.md) for the full context and phased plan. The next
contributor starts at **Stage 1 (S&P 500)**.
