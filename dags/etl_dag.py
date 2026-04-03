import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from extract import read_clients, read_instruments, read_trades
from transform import transform_clients, transform_instruments, transform_trades
from load import run_load
from storage import ensure_bucket, upload_df, download_df


def task_extract(**_):
    ensure_bucket()
    clients = read_clients()
    instruments = read_instruments()
    trades = read_trades()
    upload_df(clients,      "raw/clients.csv")
    upload_df(instruments,  "raw/instruments.csv")
    upload_df(trades,       "raw/trades.csv")
    print(f"Extracted: {len(clients)} clients, {len(instruments)} instruments, {len(trades)} trades")


def task_transform(**_):
    clients_raw     = download_df("raw/clients.csv")
    instruments_raw = download_df("raw/instruments.csv")
    trades_raw      = download_df("raw/trades.csv")

    clients     = transform_clients(clients_raw)
    instruments = transform_instruments(instruments_raw)
    trades, quarantine = transform_trades(trades_raw, clients, instruments)

    upload_df(clients,     "processed/clients.csv")
    upload_df(instruments, "processed/instruments.csv")
    upload_df(trades,      "processed/trades.csv")
    upload_df(quarantine,  "processed/quarantine.csv")
    print(
        f"Transformed: {len(clients)} clients, {len(instruments)} instruments, "
        f"{len(trades)} clean trades, {len(quarantine)} quarantined"
    )


def task_load(**_):
    clients     = download_df("processed/clients.csv")
    instruments = download_df("processed/instruments.csv")
    trades      = download_df("processed/trades.csv")
    quarantine  = download_df("processed/quarantine.csv")
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
