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

params = {
    'pdbid': '7cpa',
    'ligand_db': 'sweetlead',
    'ligands_chunk_size': 1000,
    'n_batches': 10,
}

"""
Wrapper around KubernetesPodOperator to allow parametrization of 
command.
"""
class PrepareLigandsOperator(KubernetesPodOperator):
    template_fields = (*KubernetesPodOperator.template_fields, 'batch_label')

    def __init__(self, batch_label: str, **kwargs):
        self.batch_label = batch_label

        super().__init__(
            **kwargs
        )
        

    def execute(self, context):
        return super().execute(context)

"""
Wrapper around KubernetesPodOperator to allow parametrization of 
command.
"""
class PerformDockingOperator(KubernetesPodOperator):
    template_fields = (*KubernetesPodOperator.template_fields, 'batch_label')

    def __init__(self, batch_label: str, **kwargs):
        super().__init__(**kwargs)
        self.batch_label = batch_label

    def pre_execute(self, context):
        self.arguments = [
            'echo "perform_docking({}, {})"; sleep 6'.format(
                context['params']['pdbid'], self.batch_label
            )
        ]
        # return super().pre_execute(context)

@dag(start_date=datetime(2021, 1, 1), 
    schedule=None, 
    params=params)
def test_dag(): 
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

    pod_spec      = k8s.V1PodSpec(containers=[container], volumes=[volume])
    full_pod_spec = k8s.V1Pod(spec=pod_spec)

    pod_spec_gpu      = k8s.V1PodSpec(containers=[container], volumes=[volume], runtime_class_name='nvidia')
    full_pod_spec_gpu = k8s.V1Pod(spec=pod_spec_gpu)

    # 1a - Prepare the protein
    prepare_receptor = KubernetesPodOperator(
        task_id='prepare_receptor',
        full_pod_spec=full_pod_spec,

        cmds=['/bin/sh', '-c'],
        arguments=['echo "fetch_prepare_protein({{ params.pdbid }})"; sleep 10'],
    )

    # split_sdf: <n> <db_label> ->  N_batches
    split_sdf = KubernetesPodOperator(
        task_id='split_sdf',
        full_pod_spec=full_pod_spec,
        do_xcom_push=True,

        cmds=['/bin/sh', '-c'],
        arguments=['echo "split_sdf({{ params.ligands_chunk_size }} {{ params.ligand_db }})"; sleep 5; echo {{ params.n_batches }} > /airflow/xcom/return.json'],
    )

    postprocessing = KubernetesPodOperator(
        task_id='postprocessing',
        full_pod_spec=full_pod_spec,

        cmds=['/bin/sh', '-c'],
        arguments=['echo "postprocessing({{ params.pdbid }}, {{ params.ligand_db }})"; sleep 3'],
    )

    @task
    def get_batch_labels(db_label: str, n: int):
        return [f'{db_label}_batch{i}' for i in range(n+1)]

    @task_group
    def docking(batch_label: str):

        @task
        def get_prepare_ligands_cmd(batch_label, params=None): 
            cmd = 'echo "prepare_ligands({}, {})"; sleep 8'.format(
                params["pdbid"], batch_label
            )

            return ['/bin/sh', '-c', cmd]

        prepare_ligands = KubernetesPodOperator(
            task_id='prepare_ligands',
            full_pod_spec=full_pod_spec,
            get_logs=True,

            cmds=get_prepare_ligands_cmd(batch_label)
        )

        @task
        def get_perform_docking_cmd(batch_label, params=None):
            cmd = 'echo "perform_docking({}, {})"; sleep {}'.format(
                params["pdbid"], batch_label, random.randint(15, 30)
            )

            return ['/bin/sh', '-c', cmd]

        perform_docking = PerformDockingOperator(
            task_id='perform_docking',
            full_pod_spec=full_pod_spec_gpu,
            container_resources=k8s.V1ResourceRequirements(
                limits={"nvidia.com/gpu": "1"}
            ),
            pool='gpu_pool',

            cmds=['/bin/sh', '-c'],
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