# Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.configuration import conf
from kubernetes.client import models as k8s

params = {
    'pdbid': '7cpa'
}

@dag(start_date=datetime(2021, 1, 1),
     schedule=None,
     catchup=False,
     params=params)



## Here we define three dummy tasks and we get 
[t1, t2] >> t3 >> t4