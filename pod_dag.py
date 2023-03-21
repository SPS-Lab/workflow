from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

def helloworld1():
    print('Hello World - 1')

def helloworld2():
    print("The time of code execution begin is : ", time.ctime())
    print('Hello World - 2')

with DAG(dag_id="pod",
         start_date=datetime(2021,1,1),
         schedule=None,
         catchup=False) as dag:

         task1 = PythonOperator(
             task_id="hello1",
             python_callable=helloworld1)

         task2 = PythonOperator(
             task_id="hello2",
             python_callable=helloworld2)
         
         k = KubernetesPodOperator(
             name="hello-dry-run",
             image="debian",
             cmds=["bash", "-cx"],
             arguments=["echo", "10"],
             labels={"foo": "bar"},
             task_id="dry_run_demo",
             do_xcom_push=True,
         )

         k.dry_run()

