from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'shereen',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'arxiv_end_to_end_pipeline',
    default_args=default_args,
    description='Run Fetch and Spark Scripts inside Docker via Airflow',
    schedule_interval='@weekly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    
    task_fetch = BashOperator(
        task_id='run_fetch_data_script',
        bash_command='python /opt/airflow/scripts/fetch_data.py',
    )

    
    task_spark_analytics = BashOperator(
        task_id='run_spark_analytics',
        bash_command='python /opt/airflow/scripts/spark.py',
    )

    
    task_fetch >> task_spark_analytics