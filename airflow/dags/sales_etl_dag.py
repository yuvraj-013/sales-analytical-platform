from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
"owner" : "data_engineer",
"retries" : 1,
"retry_delay" : timedelta(minutes=5)}

with DAG(
    dag_id="sales_analytics_etl",
    default_args=default_args,
    description="Daily sales analytics ETL pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id="extract_orders",
        bash_command="python /opt/airflow/etl/extract_orders.py",
    )

    validate = BashOperator(
        task_id="validate_orders",
        bash_command="python /opt/airflow/etl/validate_orders.py",
    )

    transform = BashOperator(
        task_id="transform_orders",
        bash_command="python /opt/airflow/etl/transform_orders.py",
    )

    load = BashOperator(
        task_id="load_orders",
        bash_command="python /opt/airflow/etl/load_orders.py",
    )

    extract >> validate >> transform >> load