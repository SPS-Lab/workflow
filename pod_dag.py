from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

def helloworld1():
    print('Hello World - 1')

def helloworld2():
    print("The time of code execution begin is : ", time.ctime())
    print('Hello World - 2')

with DAG(dag_id="pod",
         start_date=datetime(2021,1,1),
         schedule=None,
         catchup=False,
         default_args=default_args) as dag:

         task1 = PythonOperator(
             task_id="hello1",
             python_callable=helloworld1)

         task2 = PythonOperator(
             task_id="hello2",
             python_callable=helloworld2)
         
         k = KubernetesPodOperator(
             namespace="airflow",
             name="hello-dry-run",
             image="hello-world",
             cmds=["bash", "-cx"],
             arguments=["echo", "10"],
             labels={"<pod-label>": "<label-name>"},
             task_id="task-one",
             is_delete_operator_pod=True,
             get_logs=True,
         )

         task1 >> task2 >> k

