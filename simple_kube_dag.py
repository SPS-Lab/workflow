from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def helloworld1():
    print('Hello World - 1')

def helloworld2():
    print("The time of code execution begin is : ", time.ctime())
    print('Hello World - 2')

with DAG(dag_id="hello",
         start_date=datetime(2021,1,1),
         schedule=None,
         catchup=False) as dag:

         task1 = PythonOperator(
             task_id="hello2",
             python_callable=helloworld1)

         task2 = PythonOperator(
             task_id="hello2",
             python_callable=helloworld2)
         
task1 >> task2
