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
    print("The time of code execution begin is : ", time.ctime())
    print('Hello World - 1')
    
def helloworld2():
    print("The time of code execution begin is : ", time.ctime())
    print('Hello World - 2')
 
with DAG(dag_id="autodock_dag",
         start_date=datetime(2021,1,1),
         schedule=None,
         catchup=False,
         default_args=default_args) as dag:
 
    metadata = k8s.V1ObjectMeta(name="autodock-gpu")
    container = k8s.V1Container(
        image="raijenki/autodock:1.5.3-gpu2",
        command=["/bin/sh", "-c", "/home/AutoDock-GPU/bin/./autodock_gpu_64wi --ffile /home/AutoDock-GPU/input/1stp/derived/1stp_protein.maps.fld --lfile /home/AutoDock-GPU/input/1stp/derived/1stp_ligand.pdbqt"],
        name="pod_task",
        image_pull_policy="Always",
        resources=k8s.V1ResourceRequirements(
            requests={"cpu": "1600m","memory":"5000M","nvidia.com/gpu": "1"},
            limits={"nvidia.com/gpu": "1"}
        ),
        env=[
            k8s.V1EnvVar(name="NVIDIA_VISIBLE_DEVICES", value="all"),
            k8s.V1EnvVar(name="NVIDIA_DRIVER_CAPABALITIES", value="compute,utility")
        ]
    )
    spec = k8s.V1PodSpec(restart_policy="OnFailure" , runtime_class_name = "nvidia" , containers=[container]) 
    full_pod_spec = k8s.V1Pod(metadata = metadata, spec = spec)
    
    k = KubernetesPodOperator(
        namespace=namespace,
        labels={"foo": "bar"},
        task_id="docking_task",
        full_pod_spec=full_pod_spec,
        is_delete_operator_pod=False,
        get_logs=True,
    )
    
    k
    
