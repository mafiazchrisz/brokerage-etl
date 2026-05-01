# Repository Guidelines

## Project Structure & Module Organization
Core pipeline code lives in `src/`:
- `extract.py` reads CSV inputs from `data/input/`
- `transform.py` applies cleaning, validation, and quarantine rules
- `load.py` upserts clean data into PostgreSQL
- `storage.py` handles MinIO object storage I/O

Airflow orchestration is in `dags/etl_dag.py` (`extract -> transform -> load`). Database schema setup is in `sql/init.sql`. Container/runtime setup is in `Dockerfile`, `docker-compose.yml`, and `scripts/entrypoint.sh`.

## Build, Test, and Development Commands
- `docker compose up --build -d`: Build the Airflow image and start Postgres, MinIO, and Airflow.
- `docker compose ps`: Check container health/status.
- `docker compose logs -f airflow`: Follow Airflow startup and task logs.
- `docker compose exec airflow airflow dags trigger brokerage_etl`: Trigger a run on demand.
- `docker compose exec postgres psql -U postgres -d airflow`: Open SQL shell for validation queries.
- `docker compose down`: Stop services (use `-v` only when you want to reset volumes).

## Coding Style & Naming Conventions
Use Python 3.11 and follow PEP 8:
- 4-space indentation, `snake_case` for functions/variables, `UPPER_CASE` for constants.
- Keep transforms deterministic/idempotent (re-runs should not duplicate records).
- Prefer small, focused functions with explicit column handling (as in `transform.py`).
- Preserve DAG/task naming clarity (`task_extract`, `task_transform`, `task_load`).

## Testing Guidelines
There is no dedicated automated test suite yet; validation is integration-first:
1. Start the stack and trigger `brokerage_etl`.
2. Confirm all Airflow tasks succeed.
3. Run SQL checks from `README.md` for row counts and quarantine contents.

For new logic, add targeted tests under `tests/` (use `test_*.py`) and focus on transformation edge cases (duplicates, nulls, invalid references, KYC gating).

## Commit & Pull Request Guidelines
Current history uses short, imperative, lowercase subjects (for example, `add entrypoint`, `refactor etl_dag for not using f-string`).
- Keep commits scoped to one logical change.
- Use clear subjects under ~72 characters.
- In PRs, include: change summary, impacted files/modules, validation evidence (Airflow run result and/or SQL output), and any config/env changes.

## Security & Configuration Tips
Default credentials in `docker-compose.yml` are for local development only. Do not reuse them in shared or production environments. Keep sensitive overrides in environment variables, not committed files.
