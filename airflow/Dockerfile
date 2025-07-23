FROM apache/airflow:2.9.2-python3.9

USER root

RUN mkdir -p /var/lib/apt/lists/partial && \
    echo "deb http://deb.debian.org/debian bookworm main contrib non-free" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends msopenjdk-11 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/msopenjdk-11-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir \
    minio==7.1.12 \
    pyyaml==6.0 \
    requests==2.31.0 \
    psycopg2-binary==2.9.6 \
    pyspark==3.5.6 \
    s3fs==2023.12.2 \
    dbt-postgres

WORKDIR /opt/airflow

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV PYTHONPATH=/opt/airflow/dags:/opt/airflow/pipelines:/opt/airflow/data:/opt/airflow/configs:/opt/airflow/utils:/opt/airflow/jars