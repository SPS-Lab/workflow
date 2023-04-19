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
namespace = conf.get('kubernetes_executor', 'NAMESPACE')

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

    # define a generic container, which can be used for all tasks
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
        task_id='prepare_receptor',
        full_pod_spec=full_pod_spec,

        cmds=['/autodock/scripts/1a_fetch_prepare_protein.sh', '{{ params.pdbid }}'],
    )

    prepare_ligands = KubernetesPodOperator(
        task_id='prepare_ligands',
        full_pod_spec=full_pod_spec,

        cmds=['/autodock/scripts/1b_prepare_ligands.sh'],
    )

    # redo pod specs for NVIDIA
    pod_spec_gpu      = k8s.V1PodSpec(containers=[container], volumes=[volume], runtime_class_name='nvidia')
    full_pod_spec_gpu = k8s.V1Pod(spec=pod_spec_gpu)

    docking = KubernetesPodOperator(
        task_id='docking',
        full_pod_spec=full_pod_spec_gpu,

        container_resources=k8s.V1ResourceRequirements(
            requests={"nvidia.com/gpu": "1"}
        ),
        env_vars=[
            k8s.V1EnvVar(name="NVIDIA_VISIBLE_DEVICES", value="all"),
            k8s.V1EnvVar(name="NVIDIA_DRIVER_CAPABALITIES", value="compute,utility")
        ],

        cmds=['/autodock/scripts/2_docking.sh', '{{ params.pdbid }}'],
    )

    postprocessing = KubernetesPodOperator(
        task_id='postprocessing',
        full_pod_spec=full_pod_spec,

        cmds=['/bin/sh', '-c', 'echo postprocess!!!'],
    )

    [prepare_receptor, prepare_ligands] >> docking >> postprocessing

autodock()
