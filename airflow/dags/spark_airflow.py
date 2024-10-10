from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 10),
    'retries': 0,
}

with DAG('runScript', default_args=default_args, schedule_interval='@daily') as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command='./runspark.sh',
    )


