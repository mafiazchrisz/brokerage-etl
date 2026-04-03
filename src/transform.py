import pandas as pd

VALID_SIDES = {"BUY", "SELL"}


def _parse_numeric(series):
    """Remove comma separators and convert to float. e.g. '1,950' -> 1950.0"""
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )


def transform_clients(df):
    df = df.copy()
    for col in ["client_id", "client_name", "kyc_status"]:
        df[col] = df[col].astype(str).str.strip()
    df["country"] = df["country"].astype(str).str.strip().str.upper().replace("NAN", None)
    df["kyc_status"] = df["kyc_status"].str.upper()
    df = df.dropna(subset=["client_id"])
    df = df[df["client_id"] != ""]
    df = df.drop_duplicates(subset=["client_id"], keep="last")
    return df.reset_index(drop=True)


def transform_instruments(df):
    df = df.copy()
    for col in ["instrument_id", "symbol"]:
        df[col] = df[col].astype(str).str.strip()
    for col in ["asset_class", "currency", "exchange"]:
        df[col] = df[col].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["instrument_id"])
    df = df[df["instrument_id"] != ""]
    df = df.drop_duplicates(subset=["instrument_id"], keep="last")
    return df.reset_index(drop=True)


def transform_trades(df, clients_df, instruments_df):
    df = df.copy()

    # Normalize string fields
    for col in ["trade_id", "client_id", "instrument_id", "side", "status"]:
        df[col] = df[col].astype(str).str.strip()
    df["side"] = df["side"].str.upper()
    df["status"] = df["status"].str.upper()

    # Parse numeric fields (handles comma-formatted values like "1,950")
    df["quantity"] = _parse_numeric(df["quantity"])
    df["price"] = _parse_numeric(df["price"])
    df["fees"] = _parse_numeric(df["fees"]).fillna(0.0)

    # Parse timestamps
    df["trade_time"] = pd.to_datetime(df["trade_time"], utc=True, errors="coerce")

    # Dedup: for the same trade_id keep the record with the latest trade_time (late-update pattern)
    df = (
        df.sort_values("trade_time", ascending=False)
        .drop_duplicates(subset=["trade_id"], keep="first")
        .reset_index(drop=True)
    )

    # Validate each row and build rejection reasons
    valid_clients = set(clients_df["client_id"])
    valid_instruments = set(instruments_df["instrument_id"])
    kyc_map     = clients_df.set_index("client_id")["kyc_status"].to_dict()
    country_map = clients_df.set_index("client_id")["country"].to_dict()

    reasons = []
    kyc_flags = []
    for _, row in df.iterrows():
        r = []
        if row["side"] not in VALID_SIDES:
            r.append(f"invalid side '{row['side']}'")
        if pd.isna(row["quantity"]) or row["quantity"] <= 0:
            r.append(f"invalid quantity '{row['quantity']}'")
        if pd.isna(row["price"]) or row["price"] <= 0:
            r.append(f"invalid price '{row['price']}'")
        if row["client_id"] not in valid_clients:
            r.append(f"unknown client_id '{row['client_id']}'")
        if row["instrument_id"] not in valid_instruments:
            r.append(f"unknown instrument_id '{row['instrument_id']}'")

        # KYC check: only APPROVED clients with a known country may trade
        kyc_status = kyc_map.get(row["client_id"], "UNKNOWN")
        country    = country_map.get(row["client_id"])
        if kyc_status != "APPROVED":
            r.append(f"client kyc_status is {kyc_status}")
        elif not country or str(country).strip().upper() in ("", "NAN", "NONE"):
            r.append("client country is missing — KYC data incomplete")
        kyc_flags.append(kyc_status)
        reasons.append(", ".join(r))

    df["kyc_flag"] = kyc_flags

    df["_reason"] = reasons

    clean_df = df[df["_reason"] == ""].drop(columns=["_reason"]).reset_index(drop=True)

    dirty_df = df[df["_reason"] != ""].copy()
    quarantine_df = dirty_df[[
        "trade_id", "trade_time", "client_id", "instrument_id",
        "side", "quantity", "price", "fees", "_reason",
    ]].rename(columns={"_reason": "reason"}).astype(str).reset_index(drop=True)

    return clean_df, quarantine_df
