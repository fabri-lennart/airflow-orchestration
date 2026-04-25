FROM apache/airflow:3.2.1

RUN pip install --no-cache-dir \
    duckdb \
    requests \
    pandas \
    beautifulsoup4 \
    psycopg2-binary
