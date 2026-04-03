FROM apache/airflow:2.9.2
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
USER root
COPY scripts/entrypoint.sh /scripts/entrypoint.sh
RUN chmod +x /scripts/entrypoint.sh
USER airflow
