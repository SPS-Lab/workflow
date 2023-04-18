from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime

from kubernetes.client import models as k8s


VOLUME_KEY = 'data-volume'
MOUNT_PATH = '/data'

# Parameters 
# TODO: replace with DAG parameters
PROTEIN_PDBID = '7cpa'
AUTOGRID_GRID_CENTER = (49.8363, 17.6087, 36.2723)

default_args = {}
namespace = conf.get('kubernetes', 'NAMESPACE')

@task
def detect_receptor_types():
    pass

@task
def prepare_grid():
    return None

@dag(start_date=datetime(2021, 1, 1),
     schedule=None,
     catchup=False,
     default_args=default_args)
def autogrid():
    import os.path

    medadata = k8s.V1ObjectMeta(name='autodock-gpu')

    volume = k8s.V1Volume(
        name=VOLUME_KEY,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=VOLUME_KEY)
    )
    volume_mounts = [k8s.V1VolumeMount(mount_path=MOUNT_PATH, name=VOLUME_KEY)]

    container = k8s.V1Container(
            name='autodock-container',
            image='gabinsc/autodock-gpu:1.5.3',
            volume_mounts=volume_mounts,
            image_pull_policy='Always',
            working_dir=MOUNT_PATH, # work in the shared directory
            command=['/autodock/scripts/1_fetch_prepare_protein.sh', PROTEIN_PDBID]
    )
    spec = k8s.V1PodSpec(restart_policy='OnFailure', containers=[container])
    full_pod_psec = k8s.V1Pod(metadata=metadata,spec=spec)

    k = KubernetesPodOperator(
            namespace=namespace,
            task_id='autogrid',
            full_pod_spec=k8s.V1Pod(metadata=metadata,spec=spec),
            get_logs=True
    )

    k

autogrid()
