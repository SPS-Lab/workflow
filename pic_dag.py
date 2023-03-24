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
 
with DAG(dag_id="iPIC3D_DAG", start_date=datetime(2021,1,1), schedule=None, catchup=False, default_args=default_args) as dag:
 
    metadata = k8s.V1ObjectMeta(name="pic")
    container = k8s.V1Container(
        image="raijenki/mpik8s:pic",
        command=["/bin/sh", "-c", "./home/sputniPIC ./home/inputfiles/GEM_2D_airflow.inp"],
        name="pod_task",
        image_pull_policy="Always",
        resources=k8s.V1ResourceRequirements(
            requests={"cpu": "1600m","memory":"5000M"},
        ),
    )    
    spec = k8s.V1PodSpec(restart_policy="OnFailure", containers=[container]) 
    full_pod_spec = k8s.V1Pod(metadata = metadata, spec = spec)
    
    k = KubernetesPodOperator(
        namespace=namespace,
        labels={"foo": "bar"},
        task_id="task_3",
        full_pod_spec=full_pod_spec,
        is_delete_operator_pod=False,
        get_logs=True,
    )
    k
