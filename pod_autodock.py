from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.configuration import conf
from kubernetes.client import models as k8s

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

namespace = conf.get("kubernetes", "NAMESPACE")


def helloworld1():
    print('Hello World - 1')

def helloworld2():
    print("The time of code execution begin is : ", time.ctime())
    print('Hello World - 2')

with DAG(dag_id="autodock_pod",
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
             namespace=namespace,
             image="raijenki/autodock:1.5.3-gpu2",
             cmds=["/bin/sh", "-c", "/home/AutoDock-GPU/bin/./autodock_gpu_64wi --ffile /home/AutoDock-GPU/input/1stp/derived/1stp_protein.maps.fld --lfile /home/AutoDock-GPU/input/1stp/derived/1stp_ligand.pdbqt"],
             labels={"foo": "bar"},
             image_pull_policy="Always",
             name="pod_task",
             task_id="task_3",
             is_delete_operator_pod=False,
             get_logs=True,
             container_resources=k8s.V1ResourceRequirements(
                limits={
                    'nvidia.com/gpu': 1
                },
             ),
             env_vars={
                    "NVIDIA_VISIBLE_DEVICES": "all",
                    "NVIDIA_DRIVER_CAPABALITIES": "compute,utility"
                }
         )

         task1 >> task2 >> k

