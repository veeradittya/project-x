"""
Build the DuckDB query layer at DATA/warehouse.duckdb.

This DB is a *regenerable* view over the immutable Parquet files — no data lives in it.
Delete and re-run this script anytime; source of truth is always ASSET/<symbol>/bars_1min/.
"""
import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DATA_ROOT = Path(os.getenv("DATA_ROOT") or ROOT)
DB = ROOT / "warehouse.duckdb"

con = duckdb.connect(str(DB))

# Glob pattern for all symbols under ASSET/<SYMBOL>/bars_1min/...
glob = str(DATA_ROOT / "ASSET" / "*" / "bars_1min" / "**" / "*.parquet")

con.execute("CREATE SCHEMA IF NOT EXISTS asset;")
con.execute(f"""
    CREATE OR REPLACE VIEW asset.bars_1min AS
    SELECT * FROM read_parquet('{glob}', hive_partitioning=true);
""")

# Summary view
con.execute("""
    CREATE OR REPLACE VIEW asset.bars_1min_summary AS
    SELECT
      symbol,
      COUNT(*) AS bars,
      MIN(ts_utc) AS first_bar,
      MAX(ts_utc) AS last_bar,
      COUNT(DISTINCT CAST(ts_utc AS DATE)) AS trading_days,
      SUM(volume) AS total_volume
    FROM asset.bars_1min
    GROUP BY symbol
    ORDER BY symbol;
""")

print(f"Warehouse built at {DB}")
print("\nViews:")
print(con.sql("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema='asset'").df().to_string(index=False))
print("\nSummary:")
print(con.sql("SELECT * FROM asset.bars_1min_summary").df().to_string(index=False))
