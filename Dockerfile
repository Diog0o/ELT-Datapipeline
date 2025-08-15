FROM apache/airflow:2.7.3
USER root

# Install Docker CLI
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install required providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.7.2 \
    apache-airflow-providers-postgres==5.6.0