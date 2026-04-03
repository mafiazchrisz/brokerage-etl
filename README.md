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

## Prerequisites

- Docker Desktop (with Compose v2)

## Quick Start

```bash
docker compose up --build -d
```

First boot takes ~60 seconds while Airflow initialises and pip installs packages.

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | `admin / admin` |
| MinIO Console | http://localhost:9001 | `minioadmin / minioadmin` |

## Trigger a Run

**Via UI:** toggle the `brokerage_etl` DAG **On**, then click **Trigger DAG ▶**.

**Via CLI:**
```bash
docker compose exec airflow airflow dags trigger brokerage_etl
```

## Confirm Results

Connect to the database:
```bash
docker compose exec postgres psql -U postgres -d airflow
```

```sql
-- Set schema so table names don't need to be qualified
SET search_path TO brokerage;

-- Row counts per table
SELECT 'clients'         AS tbl, COUNT(*) FROM clients
UNION ALL
SELECT 'instruments',             COUNT(*) FROM instruments
UNION ALL
SELECT 'trades',                  COUNT(*) FROM trades
UNION ALL
SELECT 'quarantine_trades',       COUNT(*) FROM quarantine_trades;

-- Clean trades with client and instrument detail
SELECT t.trade_id, t.trade_time, c.client_name, i.symbol,
       t.side, t.quantity, t.price, t.fees, t.status
FROM   trades t
JOIN   clients     c USING (client_id)
JOIN   instruments i USING (instrument_id)
ORDER  BY t.trade_time;

-- Rejected trades and why
SELECT trade_id, reason FROM quarantine_trades ORDER BY trade_id;
```

Re-running the DAG produces the same result (idempotent — all loads use `ON CONFLICT DO UPDATE`).

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| `airflow standalone` | Single process (webserver + scheduler). Minimal containers for local dev. |
| MinIO as staging layer | Raw and processed files land in object storage (`brokerage/raw/`, `brokerage/processed/`) before touching the DB. Files are browsable via MinIO Console without a SQL client. |
| Two-stage landing (`raw/` → `processed/`) | Separating raw from processed enables re-processing without re-extracting, and keeps an audit trail of the original data. |
| `brokerage` schema inside the `airflow` DB | Avoids a second `CREATE DATABASE` call that would require a shell init script (and CRLF risk on Windows). |
| Quarantine table | Invalid rows are logged with a reason instead of silently dropped — supports traceability. |
| `ON CONFLICT DO UPDATE` everywhere | Safe to re-run; running twice gives the same final state. |
| Late updates (T0034) | Duplicate `trade_id` with different `trade_time` → keep the record with the latest timestamp. |
| `null fees → 0` | Cancelled trades commonly carry no fee; treating null as 0 is safer than rejecting the trade. |
| No FK constraints in DB | FK validation is done in Python transform; avoids insert-order issues between tables. |
| KYC gate on trades | Only clients with `kyc_status = APPROVED` **and** a known `country` may have trades loaded. PENDING/REJECTED or missing country → quarantine. Country is required for sanctions screening (AML/OFAC). |

## Data Quality Rules

Trades are quarantined (not silently dropped) when:
- `side` is not `BUY` or `SELL`
- `quantity` ≤ 0 or missing
- `price` ≤ 0 or missing
- `client_id` not found in the clients reference table
- `instrument_id` not found in the instruments reference table
- `kyc_status` is not `APPROVED` (PENDING and REJECTED clients cannot trade)
- `kyc_status` is `APPROVED` but `country` is missing (KYC data incomplete — country is required for sanctions screening)
