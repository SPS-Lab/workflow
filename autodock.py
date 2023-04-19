from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from datetime import datetime

from kubernetes.client import models as k8s

PVC_NAME = 'pvc-autodock'
MOUNT_PATH = '/data'
VOLUME_KEY  = 'volume-autodock'

# Parameters 
# TODO: replace with DAG parameters
AUTOGRID_GRID_CENTER = (49.8363, 17.6087, 36.2723)

default_args = {
    'pdbid': '7cpa'
}
namespace = conf.get('kubernetes', 'NAMESPACE')

@dag(start_date=datetime(2021, 1, 1),
     schedule=None,
     catchup=False,
     default_args=default_args)
def autodock(): 
    import os.path

    volume = k8s.V1Volume(
        name=VOLUME_KEY,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME)
    )
    volume_mount = k8s.V1VolumeMount(mount_path=MOUNT_PATH, name=VOLUME_KEY)

    prepare_receptor = KubernetesPodOperator(
            namespace=namespace,
            task_id='prepare_receptor',

            image='gabinsc/autodock-gpu:1.5.3',
            cmds=['sh', '-c'],
            arguments=['cd ' + MOUNT_PATH + ' && /autodock/scripts/1_fetch_prepare_protein.sh {{ params.pdbid }}'],

            volume_mounts=[volume_mount],
            volumes=[volume],
            image_pull_policy='Always',
    )

    prepare_receptor

autodock()
