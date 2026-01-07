from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from twitter_etl import run_twitter_etl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["xyz@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="twitter_dag",
    default_args=default_args,
    description="ETL DAG for Twitter data using X API v2",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    run_etl = PythonOperator(
        task_id="run_twitter_etl",
        python_callable=run_twitter_etl,
    )

    run_etl
