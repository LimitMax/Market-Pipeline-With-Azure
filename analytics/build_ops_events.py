import duckdb

DB_PATH = "analytics.duckdb"
OPS_EVENTS_PATH = "data/lake_snapshot/ops_pipeline_events/**/*.parquet"


def build_ops_pipeline_events(con):
    con.execute("DROP TABLE IF EXISTS ops_pipeline_events")

    con.execute(f"""
    CREATE TABLE ops_pipeline_events AS
    SELECT
        pipeline_run_id,
        pipeline_name,
        event_type,
        step,
        asset,
        asset_type,
        reason,
        execution_date,
        event_time
    FROM read_parquet('{OPS_EVENTS_PATH}');
    """)


if __name__ == "__main__":
    con = duckdb.connect(DB_PATH)
    build_ops_pipeline_events(con)
    con.close()
