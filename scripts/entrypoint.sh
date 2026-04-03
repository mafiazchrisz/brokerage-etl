#!/bin/bash
set -e

# Start Airflow standalone in background
airflow standalone &
AIRFLOW_PID=$!

# Wait until the webserver is healthy, then fix the password
echo "Waiting for Airflow webserver to be ready..."
until curl -sf "http://localhost:8080/health" > /dev/null 2>&1; do
    sleep 5
done

# Reset password to the value set in env
airflow users reset-password \
    --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}"

echo "Airflow is ready. Login: ${_AIRFLOW_WWW_USER_USERNAME:-admin} / ${_AIRFLOW_WWW_USER_PASSWORD:-admin}"

# Keep container alive
wait "$AIRFLOW_PID"