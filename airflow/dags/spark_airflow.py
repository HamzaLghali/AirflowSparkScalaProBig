from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 10),
    'retries': 0,
}


with DAG(


    dag_id="dagy",
    start_date=datetime(2024,10,14),
    schedule="@daily",
):
    

 bash_task = BashOperator(
    task_id="simple",
    bash_command="runspark.sh")
