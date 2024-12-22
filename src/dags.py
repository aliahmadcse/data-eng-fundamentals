# dag means Directed Acyclic Graph

import airflow
from src.transformation import *
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def etl():
    movies_df = load_df(table_name="movies")
    ratings_df = load_df(table_name="ratings")
    joined_df = transform_avg_ratings(movies_df, ratings_df)
    load_df_to_db(joined_df)


default_args = {
    "owner": "ali",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": True,
    "email": ["info@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "etl_pipeline",
    default_args=default_args,
    description="A simple ETL pipeline",
    schedule_interval="0 0 * * *"
)


etl_task = PythonOperator(
    task_id="etl_task",
    python_callable=etl,
    dag=dag
)
