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


from datetime import datetime

def start_gpu_container(**kwargs):

    # get the docker params from the environment
    client = docker.from_env()
          
    # run the container
    response = client.containers.run(

        # The container you wish to call
        'tensorflow/tensorflow:latest-gpu',

        # The command to run inside the container
        'nvidia-smi',
        
        # Passing the GPU access
        device_requests=[
            docker.types.DeviceRequest(count=-1, capabilities=[['gpu']])
        ]
    )

    return str(response)

    
with DAG(dag_id="autodock_pod",
         start_date=datetime(2021,1,1),
         schedule=None,
         catchup=False,
         default_args=default_args) as dag:
    
    task1 = PythonOperator(
             task_id="gpu_test",
             python_callable=start_gpu_container)

task1
