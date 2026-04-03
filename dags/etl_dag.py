from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from extract import read_clients, read_instruments, read_trades
from transform import transform_clients, transform_instruments, transform_trades
from load import run_load
from storage import ensure_bucket, upload_df, download_df

def _raw(ds, name):
    return f"raw/{ds}/{name}.csv"

def _processed(ds, name):
    return f"processed/{ds}/{name}.csv"

def task_extract(ds, **_):
    ensure_bucket()
    clients = read_clients()
    instruments = read_instruments()
    trades = read_trades()
    upload_df(clients,     _raw(ds, "clients"))
    upload_df(instruments, _raw(ds, "instruments"))
    upload_df(trades,      _raw(ds, "trades"))
    print(f"[{ds}] Extracted: {len(clients)} clients, {len(instruments)} instruments, {len(trades)} trades")


def task_transform(ds, **_):
    clients_raw     = download_df(_raw(ds, "clients"))
    instruments_raw = download_df(_raw(ds, "instruments"))
    trades_raw      = download_df(_raw(ds, "trades"))

    clients     = transform_clients(clients_raw)
    instruments = transform_instruments(instruments_raw)
    trades, quarantine = transform_trades(trades_raw, clients, instruments)

    upload_df(clients,     _processed(ds, "clients"))
    upload_df(instruments, _processed(ds, "instruments"))
    upload_df(trades,      _processed(ds, "trades"))
    upload_df(quarantine,  _processed(ds, "quarantine"))
    print(
        f"[{ds}] Transformed: {len(clients)} clients, {len(instruments)} instruments, "
        f"{len(trades)} clean trades, {len(quarantine)} quarantined"
    )

def task_load(ds, **_):
    clients     = download_df(_processed(ds, "clients"))
    instruments = download_df(_processed(ds, "instruments"))
    trades      = download_df(_processed(ds, "trades"))
    quarantine  = download_df(_processed(ds, "quarantine"))
    run_load(clients, instruments, trades, quarantine)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="brokerage_etl",
    default_args=default_args,
    start_date=datetime(2026, 3, 9),
    schedule="@daily",
    catchup=False,
    tags=["brokerage", "etl"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=task_extract)
    transform = PythonOperator(task_id="transform", python_callable=task_transform)
    load = PythonOperator(task_id="load", python_callable=task_load)

    extract >> transform >> load