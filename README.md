# Brokerage ETL Pipeline

Daily ETL pipeline that ingests brokerage CSV data, cleans it, and loads it into PostgreSQL.

## Architecture

```
data/input/*.csv
      │
  [extract]   → upload raw files  → MinIO: brokerage/raw/
      │
  [transform] → download raw/     → MinIO: brokerage/processed/
      │
  [load]      → download processed/ → upsert into PostgreSQL (brokerage schema)
```

Orchestrated by Apache Airflow (scheduled `@daily`, retriable on failure).

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Orchestration | Apache Airflow (standalone) | 2.9.2 |
| Object Storage | MinIO (S3-compatible) | latest |
| Database | PostgreSQL | 15 |
| Language | Python | 3.11 |
| Data processing | pandas | 2.2.2 |

## Project Structure

```
├── dags/
│   └── etl_dag.py          # Airflow DAG — extract → transform → load
├── src/
│   ├── extract.py           # Read source CSVs
│   ├── transform.py         # Clean, validate, split clean/quarantine
│   ├── load.py              # Upsert into PostgreSQL
│   └── storage.py           # MinIO upload/download helpers
├── sql/
│   └── init.sql             # Schema and table definitions
├── scripts/
│   └── entrypoint.sh        # Airflow startup script
├── data/input/              # Source CSV files
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Prerequisites

- Docker Desktop
- DBeaver *(optional — for browsing PostgreSQL via GUI)*

## Setup

### Step 1 — Clone the repository

```bash
git clone https://github.com/mafiazchrisz/junior-de-mar2026.git
cd junior-de-mar2026
```

### Step 2 — Start all services

```bash
docker compose up --build -d
```

This will build the Airflow image, pull PostgreSQL and MinIO, and start all 3 containers.

### Step 3 — Wait for services to be ready

First boot takes ~60–90 seconds. Check that all containers are healthy:

```bash
docker compose ps
```

You can also follow the Airflow startup log until it prints `Airflow is ready`:
```bash
docker compose logs -f airflow
```

### Step 4 — Open the services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | user: `admin` / password: `admin` |
| MinIO Console | http://localhost:9001 | `minioadmin / minioadmin` |
| PostgreSQL | `localhost:5432` | user: `postgres` / password: `postgres` / db: `airflow` |

### Step 5 — Enable and trigger the DAG

1. Open **http://localhost:8080** and log in
2. Find the `brokerage_etl` DAG
3. Toggle the DAG **On** (slider on the left)
4. Click **▶ Trigger DAG** to run it immediately

Or via CLI:
```bash
docker compose exec airflow airflow dags trigger brokerage_etl
```

### Step 6 — Monitor the run

In the Airflow UI, click on the `brokerage_etl` DAG → click the latest run → verify all 3 tasks are green:
```
extract → transform → load
```

### Step 7 — Browse staged files in MinIO

Open **http://localhost:9001**, log in, then navigate to:
```
brokerage → raw → <run-date> → clients.csv / instruments.csv / trades.csv
brokerage → processed → <run-date> → clients.csv / instruments.csv / trades.csv / quarantine.csv
```

### Step 8 — Verify results

See [Confirm Results](#confirm-results) below for SQL queries.

## Confirm Results

Connect to the database:
```bash
docker compose exec postgres psql -U postgres -d airflow
```

```sql
-- ── 1. Row counts ────────────────────────────────────────────────────────────
SELECT 'clients'          AS tbl, COUNT(*) FROM brokerage.clients
UNION ALL
SELECT 'instruments',              COUNT(*) FROM brokerage.instruments
UNION ALL
SELECT 'trades',                   COUNT(*) FROM brokerage.trades
UNION ALL
SELECT 'quarantine_trades',        COUNT(*) FROM brokerage.quarantine_trades;

-- ── 2. Clean trades (full detail) ───────────────────────────────────────────
SELECT t.trade_id,
       t.trade_time,
       c.client_name,
       c.kyc_status,
       i.symbol,
       i.asset_class,
       t.side,
       t.quantity,
       t.price,
       t.fees,
       t.status
FROM   brokerage.trades t
JOIN   brokerage.clients     c USING (client_id)
JOIN   brokerage.instruments i USING (instrument_id)
ORDER  BY t.trade_time;

-- ── 3. Quarantined trades and reasons ───────────────────────────────────────
SELECT trade_id, reason, quarantined_at
FROM   brokerage.quarantine_trades
ORDER  BY trade_id;
```

Re-running the DAG produces the same result (idempotent — all loads use `ON CONFLICT DO UPDATE`).

## Design Decisions

**Infrastructure**
- **`airflow standalone`** — Single process (webserver + scheduler). Avoids running 4+ containers for a local dev setup.
- **MinIO as staging layer** — Raw and processed files land in object storage (`brokerage/raw/<date>/`, `brokerage/processed/<date>/`) before touching the DB. Browsable via MinIO Console without a SQL client.
- **`brokerage` schema inside the `airflow` DB** — Avoids a second `CREATE DATABASE` call; simpler init script with no CRLF issues on Windows.

**Pipeline behaviour**
- **Two-stage landing (`raw/` → `processed/`)** — Separates source data from cleaned data. Enables re-processing a specific date without re-extracting.
- **`ON CONFLICT DO UPDATE` everywhere** — All upserts are idempotent; running the DAG twice produces the same result.
- **No FK constraints in DB** — FK validation is done in Python transform to avoid insert-order dependency between tables.

**Data handling**
- **Duplicate `trade_id` (T0034)** — Late-update pattern: when the same `trade_id` appears twice, keep the record with the latest `trade_time`.
- **`null fees → 0`** — Cancelled trades commonly carry no fee; treating null as 0 avoids rejecting otherwise valid trades.
- **Quarantine table** — Invalid rows are stored with a reason instead of being silently dropped, supporting auditability and investigation.

**KYC**
- **KYC gate on trades** — Only clients with `kyc_status = APPROVED` **and** a non-null `country` may have trades loaded. `country` is required for sanctions screening (AML/OFAC). PENDING, REJECTED, or missing country → quarantine.

## Data Quality Rules

Trades are quarantined (not silently dropped) when any of the following apply:

**Field validation**
- `side` is not `BUY` or `SELL`
- `quantity` ≤ 0 or missing
- `price` ≤ 0 or missing

**Reference integrity**
- `client_id` not found in the clients table
- `instrument_id` not found in the instruments table

**KYC compliance**
- `kyc_status` is `PENDING` or `REJECTED` — client not cleared to trade
- `kyc_status` is `APPROVED` but `country` is missing — KYC data incomplete
