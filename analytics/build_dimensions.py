import duckdb
from datetime import datetime

DB_PATH = "analytics.duckdb"

def build_dim_datetime(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_datetime AS
    SELECT
        CAST(strftime(ts, '%Y%m%d%H') AS BIGINT) AS datetime_key,
        ts AS datetime_utc,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(dow FROM ts) AS day_of_week,
        CASE
            WHEN EXTRACT(dow FROM ts) IN (0,6) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM generate_series(
        TIMESTAMP '2025-01-01 00:00:00',
        now(),
        INTERVAL '1 hour'
    ) AS t(ts);
    """)

def build_dim_asset(con):
    assets = [
        ("BTC-USD", "crypto"),
        ("ETH-USD", "crypto"),
        ("BNB-USD", "crypto"),
        ("SOL-USD", "crypto"),
        ("ADA-USD", "crypto"),
    ]

    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_asset (
        asset_key INTEGER,
        asset_symbol VARCHAR,
        asset_type VARCHAR
    );
    """)

    con.execute("DELETE FROM dim_asset")

    for i, (symbol, asset_type) in enumerate(assets, start=1):
        con.execute(
            "INSERT INTO dim_asset VALUES (?, ?, ?)",
            [i, symbol, asset_type]
        )

if __name__ == "__main__":
    con = duckdb.connect(DB_PATH)
    build_dim_datetime(con)
    build_dim_asset(con)
    con.close()
