# Custom Airflow image with ELT pipeline dependencies pre-installed
FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libaio-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Best-effort: install python-oracledb (may be blocked by corporate proxy)
RUN pip install --no-cache-dir python-oracledb==2.5.0 || \
    echo "WARNING: python-oracledb could not be installed. Oracle DAG tasks will fail until it is available."

# Copy the src package so Airflow hooks/operators can import it
COPY src/ /opt/airflow/src/
COPY airflow/plugins/ /opt/airflow/plugins/
COPY airflow/dags/ /opt/airflow/dags/
