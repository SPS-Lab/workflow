from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task, task_group

from datetime import datetime
import random

from airflow.configuration import conf
from airflow import XComArg
from airflow.models.xcom_arg import MapXComArg

from typing import Sequence
from kubernetes.client import models as k8s

PVC_NAME = 'pvc-autodock'
MOUNT_PATH = '/data'
VOLUME_KEY  = 'volume-autodock'

namespace = conf.get('kubernetes_executor', 'NAMESPACE')

class BaseAutoDockPodOperator(KubernetesPodOperator):
    volume = k8s.V1Volume(
        name=VOLUME_KEY,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
    )
    volume_mount = k8s.V1VolumeMount(mount_path=MOUNT_PATH, name=VOLUME_KEY)

    # define a generic container, which can be used for all tasks
    container = k8s.V1Container(
        name='autodock-container',
        image='alpine',
        working_dir=MOUNT_PATH,

        volume_mounts=[volume_mount],
        image_pull_policy='Always',
    )

    def __init__(self, **kwargs):
        super().__init__(
            namespace=namespace,
            cmds=['/bin/sh', '-c'],
            **kwargs
        )

class GenericAutoDockPodOperator(BaseAutoDockPodOperator):
    def __init__(self, **kwargs):
        pod_spec      = k8s.V1PodSpec(containers=[self.container], volumes=[self.volume])
        full_pod_spec = k8s.V1Pod(spec=pod_spec)

        super().__init__(
            full_pod_spec=full_pod_spec,
            **kwargs
        )

class GpuAutoDockPodOperator(BaseAutoDockPodOperator):
    def __init__(self, **kwargs):
        pod_spec      = k8s.V1PodSpec(containers=[self.container], volumes=[self.volume], runtime_class_name='nvidia')
        full_pod_spec = k8s.V1Pod(spec=pod_spec)

        super().__init__(
            container_resources=k8s.V1ResourceRequirements(
                limits={"nvidia.com/gpu": "1"}
            ),
            full_pod_spec=full_pod_spec,
            pool='gpu_pool',
            **kwargs
        )

class PrepareLigandOperator(GenericAutoDockPodOperator):
    template_fields = (*KubernetesPodOperator.template_fields, "batch_label")

    def __init__(self, batch_label: str, **kwargs):
        super().__init__(**kwargs)
        self.batch_label = batch_label

    def execute(self, context):
        self.arguments = [
            f'echo "prepare_ligands({ context["params"]["pdbid"] }, { self.batch_label })"; sleep 1'
        ]
        super().execute(context)

class PerformDockingOperator(GpuAutoDockPodOperator):
    template_fields = (*GpuAutoDockPodOperator.template_fields, "batch_label")

    def __init__(self, batch_label: str, **kwargs):
        super().__init__(**kwargs)
        self.batch_label = batch_label

    def execute(self, context):
        self.arguments = [
            f'echo "perform_docking({ context["params"]["pdbid"] }, { self.batch_label })"; sleep 6'
        ]
        super().execute(context)

params = {
    'pdbid': '7cpa',
    'ligand_db': 'sweetlead',
    'ligands_chunk_size': 1000,
}

@dag(start_date=datetime(2021, 1, 1), 
    schedule=None, 
    params=params)
def test_dag(): 

    # 1a - Prepare the protein
    prepare_receptor = GenericAutoDockPodOperator(
        task_id='prepare_receptor',

        arguments=['echo "fetch_prepare_protein({{ params.pdbid }})"; sleep 1'],
    )

    # split_sdf: <n> <db_label> ->  N_batches
    split_sdf = GenericAutoDockPodOperator(
        task_id='split_sdf',
        do_xcom_push=True,

        arguments=['echo "split_sdf({{ params.ligands_chunk_size }} {{ params.ligand_db }})"; sleep 3; echo 2 > /airflow/xcom/return.json'],
    )

    postprocessing = GenericAutoDockPodOperator(
        task_id='postprocessing',

        arguments=['echo "postprocessing({{ params.pdbid }}, {{ params.ligand_db }})"; sleep 2'],
    )

    @task
    def get_batch_labels(db_label: str, n: int):
        return [f'{db_label}_batch{i}' for i in range(n+1)]

    @task_group
    def docking(batch_label: str):
        
        # prepare_ligands: <db_label> <batch_num> -> filelist_<db_label>_batch<batch_num>
        prepare_ligands = PrepareLigandOperator(
            task_id='prepare_ligands',
            get_logs=True,

            batch_label=batch_label, 
        )

        # perform_docking: <filelist> -> ()
        perform_docking = PerformDockingOperator(
            task_id='perform_docking',
            get_logs=True, # otherwise generates too much lo

            batch_label=batch_label
        )

        [prepare_receptor, prepare_ligands] >> perform_docking

    # converts (db_label, n) to a list of batch_labels
    batch_labels = get_batch_labels('sweetlead', split_sdf.output)

    # for each batch_label, we create a prepare_ligand + perform_docking task
    d = docking.expand(batch_label=batch_labels)
    
    # add post-processing
    d >> postprocessing


test_dag()
    
"""# Convert 'N batches' to [batch1, batch2, ... batchN]
@task
def get_batch_labels(db_label: str, n: int):
    return [f'{db_label}_batch{i}' for i in range(n)]"""