"""Connection test: verify Alpaca keys and pull one recent day of MSFT 1-min bars."""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

key = os.environ["ALPACA_API_KEY_ID"]
secret = os.environ["ALPACA_API_SECRET_KEY"]

client = StockHistoricalDataClient(key, secret)

# Pull yesterday's 1-min bars for MSFT (small, cheap test)
end = datetime.now(timezone.utc) - timedelta(days=1)
start = end - timedelta(days=1)

req = StockBarsRequest(
    symbol_or_symbols="MSFT",
    timeframe=TimeFrame.Minute,
    start=start,
    end=end,
)

bars = client.get_stock_bars(req).df
print(f"rows returned: {len(bars)}")
print(f"columns: {list(bars.columns)}")
print(f"time range: {bars.index.get_level_values('timestamp').min()} -> {bars.index.get_level_values('timestamp').max()}")
print("\nfirst 3 bars:")
print(bars.head(3))
print("\nlast 3 bars:")
print(bars.tail(3))
