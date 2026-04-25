from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import duckdb

def extract_data(**kwargs):
    url = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/refs/heads/master/data/retail-data/all/online-retail-dataset.csv"
    df = pd.read_csv(url)
    path = "/tmp/retail_raw.parquet"
    df.to_parquet(path, index=False)
    return path

def clean_columns(**kwargs):
    ti = kwargs['ti']
    input_path = ti.xcom_pull(task_ids='extract_data')

    df = pd.read_parquet(input_path)
    df.columns = [c.lower() for c in df.columns]

    output_path = "/tmp/retail_clean.parquet"
    df.to_parquet(output_path, index=False)
    return output_path

def analyze_revenue(**kwargs):
    ti = kwargs['ti']
    clean_path = ti.xcom_pull(task_ids='clean_columns')

    con = duckdb.connect()
    query = f"""
        SELECT
            country,
            ROUND(SUM(quantity * unitprice), 2) as total_revenue
        FROM '{clean_path}'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """
    df_result = con.execute(query).df()
    print("--- RESULTADO ANALÍTICO ---")
    print(df_result)

with DAG(
    dag_id="cuack_processing",
    start_date=datetime(2026, 4, 25),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    t2 = PythonOperator(
        task_id="clean_columns",
        python_callable=clean_columns
    )

    t3 = PythonOperator(
        task_id="analyze_revenue",
        python_callable=analyze_revenue
    )

    t1 >> t2 >> t3
