"""
Fetch S&P 1500 universe (S&P 500 + MidCap 400 + SmallCap 600) from Wikipedia
and write three per-tier CSVs plus a combined sp1500.csv.

Re-run whenever you want to refresh the constituent list. Existing files are
overwritten — the committed snapshot in git acts as the dated audit trail.

Columns:
  symbol       — canonical ticker (ALPHA+DOT form, e.g. BRK.B)
  alpaca_symbol — Alpaca-compatible form (dots become slashes, e.g. BRK/B → we
                  normalize to dash-form BRK.B since Alpaca accepts both; we
                  keep the Wikipedia canonical form and let the ingester translate)
  name         — company name
  sector       — GICS sector
  tier         — sp500 | sp400 | sp600
  hq           — headquarters location
  fetched_at   — ISO timestamp the row was scraped
"""
from __future__ import annotations

import io
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

SOURCES = [
    ("sp500", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"),
    ("sp400", "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"),
    ("sp600", "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"),
]


def fetch_table(url: str) -> pd.DataFrame:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req) as r:
        html = r.read().decode()
    tables = pd.read_html(io.StringIO(html))
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        if any("symbol" in c or "ticker" in c for c in cols):
            return t
    raise RuntimeError(f"no components table found at {url}")


def normalize(df: pd.DataFrame, tier: str, fetched_at: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Find columns
    def find(*needles):
        for c in df.columns:
            lc = c.lower()
            if any(n in lc for n in needles):
                return c
        return None

    tic = find("symbol", "ticker")
    name = find("security", "company", "name")
    sector = find("gics sector", "sector")
    hq = find("headquarters")

    out = pd.DataFrame(
        {
            "symbol": df[tic].astype(str).str.strip(),
            "name": df[name].astype(str).str.strip() if name else "",
            "sector": df[sector].astype(str).str.strip() if sector else "",
            "hq": df[hq].astype(str).str.strip() if hq else "",
            "tier": tier,
            "fetched_at": fetched_at,
        }
    )
    # Drop anything that isn't a plausible ticker
    out = out[out["symbol"].str.match(r"^[A-Z][A-Z0-9.\-]*$")].reset_index(drop=True)
    return out


def main() -> None:
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    HERE.mkdir(parents=True, exist_ok=True)

    frames = []
    for tier, url in SOURCES:
        print(f"fetching {tier} from {url} …", flush=True)
        raw = fetch_table(url)
        norm = normalize(raw, tier, fetched_at)
        out_path = HERE / f"{tier}.csv"
        norm.to_csv(out_path, index=False)
        print(f"  wrote {out_path} ({len(norm)} rows)")
        frames.append(norm)

    combined = pd.concat(frames, ignore_index=True)
    # If a symbol appears in multiple tiers (rare), keep the highest tier
    # (sp500 > sp400 > sp600). Wikipedia should not have overlap but be defensive.
    tier_rank = {"sp500": 0, "sp400": 1, "sp600": 2}
    combined["_rank"] = combined["tier"].map(tier_rank)
    combined = (
        combined.sort_values(["symbol", "_rank"])
        .drop_duplicates(subset=["symbol"], keep="first")
        .drop(columns=["_rank"])
        .sort_values(["tier", "symbol"])
        .reset_index(drop=True)
    )
    out_path = HERE / "sp1500.csv"
    combined.to_csv(out_path, index=False)
    print(f"wrote {out_path} ({len(combined)} unique symbols)")
    print(combined["tier"].value_counts().to_string())


if __name__ == "__main__":
    main()
