"""
FastAPI backend for the local asset-ledger dashboard.

All heavy lifting is DuckDB over the immutable Parquet warehouse.
Start with: .venv/bin/python -m uvicorn web.server:app --host 127.0.0.1 --port 8000
"""
from __future__ import annotations

import hashlib
import json
import math
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent
ASSET_ROOT = ROOT / "ASSET"
WEB = ROOT / "web"
STATIC = WEB / "static"

# ---------- DuckDB connection (read-only over Parquet) ----------------------
# DuckDB connections aren't thread-safe across concurrent .execute() calls —
# use a cursor() per thread to get an isolated statement handle.
_base_con: Optional[duckdb.DuckDBPyConnection] = None
_con_lock = threading.Lock()


def _ensure_base() -> duckdb.DuckDBPyConnection:
    global _base_con
    with _con_lock:
        if _base_con is None:
            _base_con = duckdb.connect(":memory:")
            glob = str(ASSET_ROOT / "*" / "bars_1min" / "**" / "*.parquet")
            _base_con.execute(f"""
                CREATE VIEW bars AS
                SELECT * FROM read_parquet('{glob}', hive_partitioning=true)
            """)
        return _base_con


def db() -> duckdb.DuckDBPyConnection:
    """Return a per-call cursor so concurrent requests don't collide."""
    return _ensure_base().cursor()


# ---------- FastAPI app ------------------------------------------------------
app = FastAPI(title="Asset Ledger", version="0.1.0")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

# Timeframe -> DuckDB interval literal
TF_INTERVAL = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "30m": "30 minutes",
    "1h": "1 hour",
    "4h": "4 hours",
    "1D": "1 day",
    "1W": "1 week",
}


def _list_symbols() -> list[str]:
    """Every subdir under ASSET/ that contains bars_1min/."""
    if not ASSET_ROOT.exists():
        return []
    return sorted(
        p.name for p in ASSET_ROOT.iterdir()
        if p.is_dir() and (p / "bars_1min").exists()
    )


# ---------- routes -----------------------------------------------------------
@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
def health():
    return {"ok": True, "now_utc": datetime.now(timezone.utc).isoformat()}


@app.get("/api/assets")
def list_assets():
    """Summary row per asset: bars, date range, last price, last change%."""
    syms = _list_symbols()
    if not syms:
        return []

    rows = []
    con = db()
    for s in syms:
        r = con.execute(
            """
            SELECT
                COUNT(*) AS bars,
                MIN(ts_utc) AS first_bar,
                MAX(ts_utc) AS last_bar,
                COUNT(DISTINCT CAST(ts_utc AT TIME ZONE 'UTC' AS DATE)) AS trading_days
            FROM bars WHERE symbol = ?
            """,
            [s],
        ).fetchone()
        bars, first_bar, last_bar, trading_days = r

        last = con.execute(
            "SELECT close, volume FROM bars WHERE symbol = ? ORDER BY ts_utc DESC LIMIT 1",
            [s],
        ).fetchone()

        # Prior-session close for change%
        prev = con.execute(
            """
            WITH last_day AS (
                SELECT MAX(CAST(ts_utc AT TIME ZONE 'America/New_York' AS DATE)) AS d
                FROM bars WHERE symbol = ?
            )
            SELECT close
            FROM bars, last_day
            WHERE symbol = ?
              AND CAST(ts_utc AT TIME ZONE 'America/New_York' AS DATE) < last_day.d
            ORDER BY ts_utc DESC LIMIT 1
            """,
            [s, s],
        ).fetchone()

        last_close = last[0] if last else None
        prev_close = prev[0] if prev else None
        chg = last_close - prev_close if (last_close and prev_close) else None
        chg_pct = (chg / prev_close * 100) if chg is not None and prev_close else None

        rows.append({
            "symbol": s,
            "bars": int(bars),
            "first_bar": first_bar.isoformat() if first_bar else None,
            "last_bar": last_bar.isoformat() if last_bar else None,
            "trading_days": int(trading_days),
            "last_price": float(last_close) if last_close else None,
            "last_volume": int(last[1]) if last else None,
            "prev_close": float(prev_close) if prev_close else None,
            "change": float(chg) if chg is not None else None,
            "change_pct": float(chg_pct) if chg_pct is not None else None,
        })
    return rows


def _symbol_or_404(symbol: str) -> str:
    symbol = symbol.upper()
    if symbol not in _list_symbols():
        raise HTTPException(404, f"Unknown symbol: {symbol}")
    return symbol


@app.get("/api/assets/{symbol}/bars")
def bars(
    symbol: str,
    tf: str = Query("1D", description="Timeframe: 1m,5m,15m,30m,1h,4h,1D,1W"),
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = Query(5000, le=50000),
    indicators: str = Query("", description="Comma-sep: sma20,sma50,sma200,ema12,ema26,vwap,bb20"),
):
    symbol = _symbol_or_404(symbol)
    if tf not in TF_INTERVAL:
        raise HTTPException(400, f"Unknown timeframe {tf}")

    interval = TF_INTERVAL[tf]
    where = ["symbol = ?"]
    params: list = [symbol]
    if start:
        where.append("ts_utc >= ?")
        params.append(start)
    if end:
        where.append("ts_utc <= ?")
        params.append(end)
    where_sql = " AND ".join(where)

    # Bucket in ET for daily/weekly so a US trading day maps to the ET date
    # it belongs to (April 17 09:30 ET should bucket to April 17, not April 16).
    # For intraday, bucket in UTC (timestamps are unambiguous anyway).
    if tf in ("1D", "1W"):
        bucket_expr = f"time_bucket(INTERVAL '{interval}', ts_utc AT TIME ZONE 'America/New_York')"
    else:
        bucket_expr = f"time_bucket(INTERVAL '{interval}', ts_utc)"

    sql = f"""
        WITH resampled AS (
            SELECT
                {bucket_expr} AS ts,
                arg_min(open, ts_utc)  AS open,
                MAX(high)              AS high,
                MIN(low)               AS low,
                arg_max(close, ts_utc) AS close,
                SUM(volume)            AS volume,
                SUM(close * volume) / NULLIF(SUM(volume), 0) AS vwap_bucket,
                COUNT(*)               AS n_raw
            FROM bars
            WHERE {where_sql}
            GROUP BY 1
        )
        SELECT * FROM resampled ORDER BY ts DESC LIMIT {int(limit)}
    """
    df = db().execute(sql, params).df().iloc[::-1].reset_index(drop=True)

    if df.empty:
        return {"candles": [], "volume": [], "indicators": {}, "timeframe": tf, "symbol": symbol}

    # Lightweight Charts wants unix-seconds for intraday, YYYY-MM-DD for daily+.
    is_daily = tf in ("1D", "1W")
    if is_daily:
        t = df["ts"].dt.strftime("%Y-%m-%d")
    else:
        # DuckDB timestamps come back as microsecond precision (datetime64[us]),
        # so // 10**9 produces garbage. Use .timestamp() — unit/tz-safe.
        ts_utc = df["ts"].dt.tz_convert("UTC") if df["ts"].dt.tz is not None else df["ts"].dt.tz_localize("UTC")
        t = ts_utc.map(lambda x: int(x.timestamp())).astype(int)

    candles = [
        {"time": ti, "open": float(o), "high": float(h), "low": float(l), "close": float(c)}
        for ti, o, h, l, c in zip(t, df["open"], df["high"], df["low"], df["close"])
    ]
    vol = [
        {"time": ti, "value": int(v),
         "color": "#30d15866" if c >= o else "#ff3b3066"}
        for ti, v, c, o in zip(t, df["volume"], df["close"], df["open"])
    ]

    # Indicators (server-side)
    ind: dict[str, list] = {}
    req = [s.strip().lower() for s in indicators.split(",") if s.strip()]
    close = df["close"].astype(float)

    def line(series: pd.Series) -> list:
        return [
            {"time": ti, "value": float(v)}
            for ti, v in zip(t, series)
            if pd.notna(v) and not math.isinf(v)
        ]

    for r in req:
        if r.startswith("sma"):
            try:
                n = int(r[3:])
                ind[r] = line(close.rolling(n, min_periods=n).mean())
            except ValueError:
                pass
        elif r.startswith("ema"):
            try:
                n = int(r[3:])
                ind[r] = line(close.ewm(span=n, adjust=False).mean())
            except ValueError:
                pass
        elif r == "vwap":
            vwap_cum = (close * df["volume"]).cumsum() / df["volume"].cumsum().replace(0, np.nan)
            ind["vwap"] = line(vwap_cum)
        elif r.startswith("bb"):
            try:
                n = int(r[2:]) if len(r) > 2 else 20
                m = close.rolling(n, min_periods=n).mean()
                # Population std (ddof=0) — matches TradingView, Bloomberg, TA-Lib,
                # and John Bollinger's original spec. pandas' default is ddof=1.
                s = close.rolling(n, min_periods=n).std(ddof=0)
                ind[f"bb{n}_mid"] = line(m)
                ind[f"bb{n}_upper"] = line(m + 2 * s)
                ind[f"bb{n}_lower"] = line(m - 2 * s)
            except ValueError:
                pass
        elif r.startswith("rsi"):
            try:
                n = int(r[3:]) if len(r) > 3 else 14
                d = close.diff()
                gains  = d.clip(lower=0)
                losses = -d.clip(upper=0)
                # Wilder's smoothing (EMA with alpha = 1/n)
                ag = gains.ewm(alpha=1/n, adjust=False).mean()
                al = losses.ewm(alpha=1/n, adjust=False).mean()
                rs = ag / al.replace(0, np.nan)
                rsi = 100 - (100 / (1 + rs))
                ind[f"rsi{n}"] = line(rsi)
            except ValueError:
                pass
        elif r == "macd":
            # Standard MACD(12,26,9)
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal    = macd_line.ewm(span=9, adjust=False).mean()
            hist      = macd_line - signal
            ind["macd_line"]   = line(macd_line)
            ind["macd_signal"] = line(signal)
            ind["macd_hist"]   = line(hist)
        elif r.startswith("stoch"):
            # Stochastic (14, 3, 3): %K = 100*(close-L14)/(H14-L14), %D = SMA3(%K)
            try:
                n = int(r[5:]) if len(r) > 5 else 14
                low_n  = df["low"].rolling(n).min()
                high_n = df["high"].rolling(n).max()
                k = 100 * (close - low_n) / (high_n - low_n).replace(0, np.nan)
                d_ = k.rolling(3).mean()
                ind[f"stoch{n}_k"] = line(k)
                ind[f"stoch{n}_d"] = line(d_)
            except ValueError:
                pass

    return {
        "symbol": symbol,
        "timeframe": tf,
        "candles": candles,
        "volume": vol,
        "indicators": ind,
        "n": len(candles),
    }


@app.get("/api/assets/{symbol}/stats")
def stats(symbol: str, start: Optional[str] = None, end: Optional[str] = None):
    symbol = _symbol_or_404(symbol)
    where = ["symbol = ?"]
    params: list = [symbol]
    if start:
        where.append("ts_utc >= ?"); params.append(start)
    if end:
        where.append("ts_utc <= ?"); params.append(end)
    w = " AND ".join(where)

    # Pull daily OHLCV for proper return/drawdown math
    daily = db().execute(f"""
        SELECT
            CAST(ts_utc AT TIME ZONE 'America/New_York' AS DATE) AS day,
            arg_min(open, ts_utc) AS open,
            MAX(high) AS high, MIN(low) AS low,
            arg_max(close, ts_utc) AS close,
            SUM(volume) AS volume
        FROM bars WHERE {w}
        GROUP BY 1 ORDER BY 1
    """, params).df()

    if daily.empty:
        return {"symbol": symbol, "empty": True}

    closes = daily["close"].astype(float)
    rets = closes.pct_change().dropna()
    peak = closes.cummax()
    dd = (closes / peak - 1)
    n_days = len(daily)

    summary_row = db().execute(f"""
        SELECT COUNT(*) n, MIN(ts_utc) mn, MAX(ts_utc) mx,
               MIN(low) lo, MAX(high) hi,
               SUM(volume) v_total
        FROM bars WHERE {w}
    """, params).fetchone()

    sources = db().execute(
        f"SELECT DISTINCT source FROM bars WHERE {w} ORDER BY 1", params
    ).fetchall()
    sources_list = [r[0] for r in sources]

    return {
        "symbol": symbol,
        "n_bars": int(summary_row[0]),
        "first_ts": summary_row[1].isoformat() if summary_row[1] else None,
        "last_ts": summary_row[2].isoformat() if summary_row[2] else None,
        "price": {
            "first": float(closes.iloc[0]),
            "last": float(closes.iloc[-1]),
            "change": float(closes.iloc[-1] - closes.iloc[0]),
            "change_pct": float((closes.iloc[-1] / closes.iloc[0] - 1) * 100),
            "period_high": float(summary_row[4]),
            "period_low": float(summary_row[3]),
        },
        "returns": {
            "n_days": int(n_days),
            "mean_daily_pct": float(rets.mean() * 100) if len(rets) else None,
            "std_daily_pct": float(rets.std() * 100) if len(rets) else None,
            "annualized_vol_pct": float(rets.std() * math.sqrt(252) * 100) if len(rets) else None,
            "annualized_return_pct": float(((1 + rets.mean()) ** 252 - 1) * 100) if len(rets) else None,
            "sharpe_approx": float(rets.mean() / rets.std() * math.sqrt(252)) if len(rets) and rets.std() else None,
            "max_drawdown_pct": float(dd.min() * 100),
            "up_days": int((rets > 0).sum()),
            "down_days": int((rets < 0).sum()),
        },
        "volume": {
            "total": int(summary_row[5]),
            "mean_daily": float(daily["volume"].mean()),
            "median_daily": float(daily["volume"].median()),
        },
        "sources": sources_list,
    }


@app.get("/api/assets/{symbol}/integrity")
def integrity(symbol: str):
    symbol = _symbol_or_404(symbol)
    man_path = ASSET_ROOT / symbol / "manifest.jsonl"
    if not man_path.exists():
        return {"symbol": symbol, "manifest_exists": False, "entries": []}

    entries = [json.loads(l) for l in man_path.read_text().splitlines() if l.strip()]
    for e in entries:
        p = ROOT / e["path"]
        if not p.exists():
            e["_status"] = "missing"
            continue
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        actual = h.hexdigest()
        e["_status"] = "ok" if actual == e["sha256"] else "tampered"
        e["_actual_sha256"] = actual
        # file permission sanity
        st = p.stat()
        e["_mode"] = oct(st.st_mode & 0o777)
        e["_size"] = st.st_size

    # Last verification report
    rpt_path = ASSET_ROOT / symbol / "verification_report.jsonl"
    last_report = None
    if rpt_path.exists():
        lines = [l for l in rpt_path.read_text().splitlines() if l.strip()]
        if lines:
            last_report = json.loads(lines[-1])

    return {
        "symbol": symbol,
        "manifest_exists": True,
        "n_entries": len(entries),
        "n_ok": sum(1 for e in entries if e["_status"] == "ok"),
        "n_tampered": sum(1 for e in entries if e["_status"] == "tampered"),
        "n_missing": sum(1 for e in entries if e["_status"] == "missing"),
        "entries": entries,
        "last_verification_report": last_report,
    }


@app.get("/api/assets/{symbol}/metadata")
def metadata(symbol: str, refresh: bool = False):
    symbol = _symbol_or_404(symbol)
    cache = ASSET_ROOT / symbol / "metadata.json"

    if cache.exists() and not refresh:
        return json.loads(cache.read_text())

    # Fetch from yfinance (slow, so we cache). Don't fail hard if offline.
    try:
        import yfinance as yf
        t = yf.Ticker(symbol)
        info = t.info or {}
    except Exception as e:
        return {"symbol": symbol, "_error": f"yfinance fetch failed: {e}"}

    keep = {
        "symbol": symbol,
        "shortName": info.get("shortName"),
        "longName": info.get("longName"),
        "quoteType": info.get("quoteType"),
        "exchange": info.get("exchange"),
        "currency": info.get("currency"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "country": info.get("country"),
        "website": info.get("website"),
        "marketCap": info.get("marketCap"),
        "sharesOutstanding": info.get("sharesOutstanding"),
        "trailingPE": info.get("trailingPE"),
        "forwardPE": info.get("forwardPE"),
        "dividendYield": info.get("dividendYield"),
        "beta": info.get("beta"),
        "52WeekHigh": info.get("fiftyTwoWeekHigh"),
        "52WeekLow": info.get("fiftyTwoWeekLow"),
        "averageVolume": info.get("averageVolume"),
        "longBusinessSummary": info.get("longBusinessSummary"),
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    cache.write_text(json.dumps(keep, indent=2, default=str))
    return keep


@app.get("/api/assets/{symbol}/bars/raw")
def raw_bars(
    symbol: str,
    page: int = Query(0, ge=0),
    page_size: int = Query(200, ge=1, le=1000),
    order: str = Query("desc", pattern="^(asc|desc)$"),
):
    symbol = _symbol_or_404(symbol)
    offset = page * page_size
    df = db().execute(f"""
        SELECT ts_utc, open, high, low, close, volume, trade_count, vwap, source
        FROM bars WHERE symbol = ?
        ORDER BY ts_utc {order.upper()}
        LIMIT {page_size} OFFSET {offset}
    """, [symbol]).df()
    total = db().execute("SELECT COUNT(*) FROM bars WHERE symbol = ?", [symbol]).fetchone()[0]

    # JSON-friendly
    df["ts_utc"] = df["ts_utc"].astype(str)
    return {
        "symbol": symbol,
        "page": page,
        "page_size": page_size,
        "total": int(total),
        "rows": df.to_dict(orient="records"),
    }


@app.get("/api/overview")
def overview():
    """Dashboard-wide numbers for the header."""
    syms = _list_symbols()
    if not syms:
        return {"n_symbols": 0, "n_bars_total": 0}
    total = db().execute("SELECT COUNT(*) FROM bars").fetchone()[0]
    return {"n_symbols": len(syms), "n_bars_total": int(total), "symbols": syms}


# ---- Universe (S&P 1500) + ingest progress ----------------------------------
UNIVERSE_CSV = ROOT / "scripts" / "universes" / "sp1500.csv"


@app.get("/api/universe")
def universe():
    """S&P 1500 universe with per-symbol ingestion status.

    Cheap: just checks whether ASSET/<SYM>/bars_1min/ exists. No DuckDB scan,
    so this scales fine to 1500 rows and can poll frequently during an ingest
    run without load on the warehouse.
    """
    if not UNIVERSE_CSV.exists():
        return {"loaded": False, "n": 0, "tiers": {}, "rows": []}

    df = pd.read_csv(UNIVERSE_CSV)
    ingested = set(_list_symbols())

    rows = []
    for r in df.itertuples(index=False):
        sym = str(r.symbol)
        rows.append({
            "symbol": sym,
            "name": getattr(r, "name", "") or "",
            "sector": getattr(r, "sector", "") or "",
            "tier": getattr(r, "tier", "") or "",
            "ingested": sym in ingested,
        })

    # Also count ingested symbols that are OUTSIDE the universe (ETFs, MSFT if
    # not yet added, etc.) so the UI can surface them as "extras".
    in_universe = {r["symbol"] for r in rows}
    extras = sorted(ingested - in_universe)

    # Per-tier progress
    tiers = {}
    for t in ("sp500", "sp400", "sp600"):
        t_rows = [r for r in rows if r["tier"] == t]
        done = sum(1 for r in t_rows if r["ingested"])
        tiers[t] = {"total": len(t_rows), "done": done}

    return {
        "loaded": True,
        "n": len(rows),
        "n_ingested": sum(1 for r in rows if r["ingested"]),
        "tiers": tiers,
        "extras": extras,
        "rows": rows,
    }


# =========================================================================
# New data endpoints: corporate actions, earnings, short volume, macro, events
# =========================================================================
def _optional_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return db().execute(f"SELECT * FROM read_parquet('{path}')").df()


@app.get("/api/assets/{symbol}/corporate_actions")
def corporate_actions(symbol: str):
    symbol = _symbol_or_404(symbol)
    path = ASSET_ROOT / symbol / "corporate_actions.parquet"
    df = _optional_parquet(path)
    if df.empty:
        return {"symbol": symbol, "dividends": [], "splits": []}
    df["ts_date"] = df["ts_date"].astype(str)
    div = df[df["action_type"] == "dividend"][["ts_date", "dividend_amount"]].to_dict("records")
    sp  = df[df["action_type"] == "split"][["ts_date", "split_ratio"]].to_dict("records")
    # Trailing-12m dividend total (for yield calc on client)
    try:
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365)
        df["ts_dt"] = pd.to_datetime(df["ts_date"], utc=True)
        ttm = float(df.loc[(df["action_type"] == "dividend") & (df["ts_dt"] >= cutoff),
                           "dividend_amount"].sum())
    except Exception:
        ttm = None
    return {
        "symbol": symbol,
        "dividends": div,
        "splits": sp,
        "ttm_dividend": ttm,
        "n_dividends": len(div),
        "n_splits": len(sp),
    }


@app.get("/api/assets/{symbol}/earnings")
def earnings(symbol: str):
    symbol = _symbol_or_404(symbol)
    path = ASSET_ROOT / symbol / "earnings.parquet"
    df = _optional_parquet(path)
    if df.empty:
        return {"symbol": symbol, "rows": [], "summary": {}}
    df["earnings_ts"] = df["earnings_ts"].astype(str)
    # NaN -> None so JSON serialization doesn't explode
    df = df.replace({np.nan: None})
    rows = df.sort_values("earnings_ts", ascending=False).to_dict("records")
    beats  = int((df["eps_surprise_pct"] > 0).sum())
    misses = int((df["eps_surprise_pct"] < 0).sum())
    inline = int((df["eps_surprise_pct"] == 0).sum())
    return {
        "symbol": symbol,
        "rows": rows,
        "n": len(df),
        "summary": {
            "beats": beats, "misses": misses, "in_line": inline,
            "beat_rate_pct": round(beats / max(1, beats + misses + inline) * 100, 2),
            "last": rows[0] if rows else None,
        },
    }


@app.get("/api/assets/{symbol}/short_volume")
def short_volume(symbol: str, limit: int = Query(5000, le=20000)):
    symbol = _symbol_or_404(symbol)
    path = ASSET_ROOT / symbol / "short_volume.parquet"
    df = _optional_parquet(path)
    if df.empty:
        return {"symbol": symbol, "rows": [], "summary": {}}
    df = df.sort_values("ts_date")
    if limit and limit < len(df):
        df = df.tail(limit)
    df["ts_date"] = df["ts_date"].astype(str)
    latest = df.iloc[-1].to_dict()
    return {
        "symbol": symbol,
        "rows": df[["ts_date", "short_volume", "short_exempt_volume",
                    "total_volume", "short_pct"]].to_dict("records"),
        "summary": {
            "n": int(len(df)),
            "latest_ts_date": latest["ts_date"],
            "latest_short_pct": float(latest["short_pct"]),
            "mean_short_pct_full_history": float(df["short_pct"].mean()),
            "mean_short_pct_last_60d": float(df["short_pct"].tail(60).mean()) if len(df) >= 60 else None,
        },
    }


@app.get("/api/assets/{symbol}/events")
def events(symbol: str):
    """Combined timeline for chart markers: dividends + splits + earnings."""
    symbol = _symbol_or_404(symbol)
    out = {"symbol": symbol, "dividends": [], "splits": [], "earnings": []}

    ca_path = ASSET_ROOT / symbol / "corporate_actions.parquet"
    ca = _optional_parquet(ca_path)
    if not ca.empty:
        ca["ts_date"] = ca["ts_date"].astype(str)
        out["dividends"] = ca[ca["action_type"] == "dividend"][
            ["ts_date", "dividend_amount"]].to_dict("records")
        out["splits"] = ca[ca["action_type"] == "split"][
            ["ts_date", "split_ratio"]].to_dict("records")

    er_path = ASSET_ROOT / symbol / "earnings.parquet"
    er = _optional_parquet(er_path)
    if not er.empty:
        er["earnings_ts"] = pd.to_datetime(er["earnings_ts"], utc=True)
        er["ts_date"] = er["earnings_ts"].dt.tz_convert("America/New_York").dt.date.astype(str)
        # NaN -> None for JSON compliance
        er = er.replace({np.nan: None})
        # Determine quarter from date (fiscal quarter aligned with calendar Q for display)
        er["_month"] = pd.to_datetime(er["ts_date"]).dt.month
        er["_year"]  = pd.to_datetime(er["ts_date"]).dt.year
        er["quarter"] = ((er["_month"] - 1) // 3 + 1).astype(int).astype(str)
        # Backfill status for older files that lack the column
        if "status" not in er.columns:
            er["status"] = er["eps_actual"].apply(lambda v: "reported" if v is not None else "scheduled")
        out["earnings"] = er[["ts_date", "quarter", "eps_actual", "eps_estimate",
                              "eps_surprise_pct", "status"]].to_dict("records")
    return out


# ---- MACRO endpoints ---------------------------------------------------
MACRO_ROOT = ROOT / "MACRO"


def _list_macro_series() -> list[str]:
    if not MACRO_ROOT.exists():
        return []
    return sorted(p.name for p in MACRO_ROOT.iterdir()
                  if p.is_dir() and (p / "series.parquet").exists())


@app.get("/api/macro/series")
def macro_series_list():
    ids = _list_macro_series()
    out = []
    for sid in ids:
        path = MACRO_ROOT / sid / "series.parquet"
        r = db().execute(
            f"""SELECT COUNT(*) n, MIN(ts_date) mn, MAX(ts_date) mx,
                       MAX(value) hi, MIN(value) lo,
                       ANY_VALUE(frequency) f, ANY_VALUE(units) u, ANY_VALUE(description) d
                FROM read_parquet('{path}')"""
        ).fetchone()
        last = db().execute(
            f"SELECT value FROM read_parquet('{path}') ORDER BY ts_date DESC LIMIT 1"
        ).fetchone()
        out.append({
            "series_id": sid,
            "n": int(r[0]),
            "first": str(r[1]) if r[1] else None,
            "last_date": str(r[2]) if r[2] else None,
            "latest_value": float(last[0]) if last else None,
            "frequency": r[5], "units": r[6], "description": r[7],
            "period_high": float(r[3]) if r[3] is not None else None,
            "period_low": float(r[4]) if r[4] is not None else None,
        })
    return out


@app.get("/api/macro/series/{series_id}")
def macro_series_data(series_id: str, start: Optional[str] = None, end: Optional[str] = None):
    path = MACRO_ROOT / series_id / "series.parquet"
    if not path.exists():
        raise HTTPException(404, f"Unknown series {series_id}")
    where = []
    params: list = []
    if start: where.append("ts_date >= ?"); params.append(start)
    if end:   where.append("ts_date <= ?"); params.append(end)
    w = ("WHERE " + " AND ".join(where)) if where else ""
    df = db().execute(
        f"SELECT ts_date, value FROM read_parquet('{path}') {w} ORDER BY ts_date",
        params,
    ).df()
    df["ts_date"] = df["ts_date"].astype(str)
    return {
        "series_id": series_id,
        "n": int(len(df)),
        "rows": df.to_dict("records"),
    }


# ---- Index daily bars (SPY/QQQ/VIX/etc.) ------------------------------------
@app.get("/api/index/{symbol}/bars_1d")
def index_bars(symbol: str):
    path = ASSET_ROOT / symbol / "bars_1d.parquet"
    if not path.exists():
        raise HTTPException(404, f"No daily bars for {symbol}")
    df = db().execute(f"""
        SELECT ts_date, open, high, low, close, volume
        FROM read_parquet('{path}') ORDER BY ts_date
    """).df()
    df["ts_date"] = df["ts_date"].astype(str)
    return {"symbol": symbol, "n": len(df), "rows": df.to_dict("records")}
