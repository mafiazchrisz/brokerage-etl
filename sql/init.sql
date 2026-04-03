-- ETL tables live in a dedicated schema inside the Airflow database.
-- This file runs automatically on first postgres startup via docker-entrypoint-initdb.d.

CREATE SCHEMA IF NOT EXISTS brokerage;

CREATE TABLE IF NOT EXISTS brokerage.clients (
    client_id   VARCHAR(10)  PRIMARY KEY,
    client_name TEXT         NOT NULL,
    country     VARCHAR(5),
    kyc_status  VARCHAR(20)  NOT NULL,
    created_at  DATE         NOT NULL
);

CREATE TABLE IF NOT EXISTS brokerage.instruments (
    instrument_id VARCHAR(10)  PRIMARY KEY,
    symbol        VARCHAR(20)  NOT NULL,
    asset_class   VARCHAR(20)  NOT NULL,
    currency      VARCHAR(10)  NOT NULL,
    exchange      VARCHAR(20)  NOT NULL
);

CREATE TABLE IF NOT EXISTS brokerage.trades (
    trade_id      VARCHAR(10)   PRIMARY KEY,
    trade_time    TIMESTAMPTZ   NOT NULL,
    client_id     VARCHAR(10)   NOT NULL,
    instrument_id VARCHAR(10)   NOT NULL,
    side          VARCHAR(4)    NOT NULL,
    quantity      NUMERIC(20,6) NOT NULL,
    price         NUMERIC(20,6) NOT NULL,
    fees          NUMERIC(20,6) NOT NULL DEFAULT 0,
    status        VARCHAR(20)   NOT NULL,
    kyc_flag      VARCHAR(20)   NOT NULL DEFAULT 'APPROVED'
);

-- Invalid rows are stored here with a reason instead of being silently dropped.
-- trade_id is the PK so re-runs update rather than duplicate.
CREATE TABLE IF NOT EXISTS brokerage.quarantine_trades (
    trade_id       VARCHAR(10)  PRIMARY KEY,
    trade_time     TEXT,
    client_id      TEXT,
    instrument_id  TEXT,
    side           TEXT,
    quantity       TEXT,
    price          TEXT,
    fees           TEXT,
    reason         TEXT         NOT NULL,
    quarantined_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
