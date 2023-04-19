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

params = {
    'pdbid': '7cpa'
}
namespace = conf.get('kubernetes', 'NAMESPACE')

@dag(start_date=datetime(2021, 1, 1),
     schedule=None,
     catchup=False,
     params=params)
def autodock(): 
    import os.path

    volume = k8s.V1Volume(
        name=VOLUME_KEY,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME)
    )
    volume_mount = k8s.V1VolumeMount(mount_path=MOUNT_PATH, name=VOLUME_KEY)


    container = k8s.V1Container(
        name='autodock-container',
        image='gabinsc/autodock-gpu:1.5.3',
        working_dir=MOUNT_PATH,

        volume_mounts=[volume_mount],
        image_pull_policy='Always',
    )
    pod_spec      = k8s.V1PodSpec(containers=[container], volumes=[volume])
    full_pod_spec = k8s.V1Pod(spec=pod_spec)

    prepare_receptor = KubernetesPodOperator(
        namespace=namespace,
        task_id='prepare_receptor',
        full_pod_spec=full_pod_spec,

        cmds=['/autodock/scripts/1a_fetch_prepare_protein.sh {{ params.pdbid }}'],
    )

    prepare_receptor

autodock()
