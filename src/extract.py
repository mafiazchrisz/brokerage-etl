import os
import glob
import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
INPUT_DIR = os.path.join(DATA_DIR, "input")


def read_clients():
    return pd.read_csv(os.path.join(INPUT_DIR, "clients.csv"))


def read_instruments():
    return pd.read_csv(os.path.join(INPUT_DIR, "instruments.csv"))


def read_trades():
    pattern = os.path.join(INPUT_DIR, "trades_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No trades file found matching: {pattern}")
    latest = files[-1]
    print(f"Reading trades from: {latest}")
    return pd.read_csv(latest)
