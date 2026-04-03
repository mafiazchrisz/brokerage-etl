import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def _get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        dbname=os.getenv("DB_NAME", "airflow"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        options="-c search_path=brokerage",
    )


def upsert(df, table, pk_cols, conn):
    """Insert rows, updating existing records on primary key conflict (idempotent)."""
    if df.empty:
        return
    df = df.where(pd.notnull(df), None)  # convert NaN -> None -> NULL in postgres
    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk_cols]
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {set_clause}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()


def insert_quarantine(df, conn):
    """Upsert quarantine records so re-runs update rather than duplicate."""
    if df.empty:
        return
    values = list(df[["trade_id", "raw_data", "reason"]].itertuples(index=False, name=None))
    sql = (
        "INSERT INTO quarantine_trades (trade_id, raw_data, reason) VALUES %s "
        "ON CONFLICT (trade_id) DO UPDATE SET "
        "raw_data = EXCLUDED.raw_data, reason = EXCLUDED.reason, quarantined_at = NOW()"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()


def run_load(clients_df, instruments_df, trades_df, quarantine_df):
    conn = _get_conn()
    try:
        upsert(clients_df, "clients", ["client_id"], conn)
        print(f"  Loaded {len(clients_df)} clients")
        upsert(instruments_df, "instruments", ["instrument_id"], conn)
        print(f"  Loaded {len(instruments_df)} instruments")
        upsert(trades_df, "trades", ["trade_id"], conn)
        print(f"  Loaded {len(trades_df)} trades")
        insert_quarantine(quarantine_df, conn)
        print(f"  Quarantined {len(quarantine_df)} trades")
    finally:
        conn.close()
