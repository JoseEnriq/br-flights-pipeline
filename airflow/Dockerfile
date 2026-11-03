FROM apache/airflow:3.3.2-python3.12

# The pipeline (ingest scripts and dbt) gets its own virtual environment with
# the versions pinned in requirements.txt: Airflow and dbt pin different
# versions of the same libraries, and sharing one environment breaks one side.
USER root
RUN python -m venv /opt/pipeline-venv && chown -R airflow: /opt/pipeline-venv
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN /opt/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
