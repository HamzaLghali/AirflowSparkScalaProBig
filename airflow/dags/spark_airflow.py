from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 10),
    'retries': 0,
}


with DAG(


    dag_id="hhtpdag",
    start_date=datetime(2024,10,14),
    schedule="@daily",
):
    


    
    def get_data():
        import requests
        import json

        res = requests.get("https://randomuser.me/api/")
        res = res.json()
        res = res["results"][0]
        print(json.dumps(res, indent=3))

        return res


get_data()

