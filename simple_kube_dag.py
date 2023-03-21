from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def helloworld():
    print('Hello World!')

with DAG(dag_id="hello",
         start_date=datetime(2021,1,1),
         schedule=None,
         catchup=False) as dag:

         task1 = PythonOperator(
             task_id="hello",
             python_callable=helloworld)

task1
