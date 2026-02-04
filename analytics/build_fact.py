import duckdb

DB_PATH = "analytics.duckdb"

DATA_LAKE_PATH = "data/lake_snapshot/**/*.parquet"

def build_fact(con):
    con.execute("DROP TABLE IF EXISTS fact_market_hourly")

    con.execute(f"""
    CREATE TABLE fact_market_hourly AS
    SELECT
        a.asset_key,
        d.datetime_key,
        f.open AS open_price,
        f.high AS high_price,
        f.low AS low_price,
        f.close AS close_price,
        f.volume
    FROM read_parquet('{DATA_LAKE_PATH}') f
    JOIN dim_asset a
      ON f.asset = a.asset_symbol
    JOIN dim_datetime d
      ON f.timestamp = d.datetime_utc;
    """)

if __name__ == "__main__":
    con = duckdb.connect(DB_PATH)
    build_fact(con)
    con.close()
