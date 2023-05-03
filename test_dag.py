from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task, task_group

from datetime import datetime
import random

from airflow.configuration import conf
from airflow import XComArg
from airflow.models.xcom_arg import MapXComArg

from kubernetes.client import models as k8s

namespace = conf.get('kubernetes_executor', 'NAMESPACE')

class PrepareLigandOperator(KubernetesPodOperator):
    def __init__(self, sdf_name:str, **kwargs):
        super().__init__(
            namespace=namespace,
            image="alpine",
            cmds=["sh", "-c"],
            arguments=[f'sleep {random.randint(1,10)}; echo \\"{sdf_name}\\" > /airflow/xcom/return.json'],
            **kwargs
        )

PVC_NAME = 'pvc-autodock'
MOUNT_PATH = '/data'
VOLUME_KEY  = 'volume-autodock'

params = {
    'pdbid': '7cpa',
    'ligand_db': 'sweetlead',
    'ligands_chunk_size': 1000,
}
namespace = conf.get('kubernetes_executor', 'NAMESPACE')

@dag(start_date=datetime(2021, 1, 1), 
    schedule=None, default_args=params)
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

    # CPU/generic pod specification
    pod_spec      = k8s.V1PodSpec(containers=[container], volumes=[volume])
    full_pod_spec = k8s.V1Pod(spec=pod_spec)

    # GPU-specific pod specification
    pod_spec_gpu      = k8s.V1PodSpec(containers=[container], volumes=[volume], runtime_class_name='nvidia')
    full_pod_spec_gpu = k8s.V1Pod(spec=pod_spec_gpu)

    # 1a - Prepare the protein
    prepare_receptor = KubernetesPodOperator(
        task_id='prepare_receptor',
        full_pod_spec=full_pod_spec,

        cmds=['/bin/sh', '-c', 'echo "fetch_prepare_protein({{ params.pdbid }})"; sleep 1'],
    )

    # split_sdf: <n> <db_label> ->  N_batches
    split_sdf = KubernetesPodOperator(
        task_id='split_sdf',
        full_pod_spec=full_pod_spec,

        cmds = ['/bin/sh', '-c'],
        arguments=['echo "split_sdf({{ params.ligands_chunk_size }} {{ params.ligand_db }})"; sleep 3; echo 5 > /airflow/xcom/return.json'],
        do_xcom_push=True,
    )

    postprocessing = KubernetesPodOperator(
        task_id='postprocessing',
        full_pod_spec=full_pod_spec,

        cmds=['/bin/sh', '-c'],
        arguments=['echo "postprocessing({{ params.pdbid }}, {{ params.ligand_db }})"; sleep 2'],
    )

    @task
    def get_batch_labels(db_label: str, n: int):
        return [f'{db_label}_batch{i}' for i in range(n+1)]

    @task_group
    def docking(batch_label: str):
        @task
        def get_prepare_ligands_cmd(batch_label, params=None): 
            return ['/bin/sh', '-c', f'echo "prepare_ligands({ params["pdbid"] }, {batch_label})"; sleep 1']
        
        # prepare_ligands: <db_label> <batch_num> -> filelist_<db_label>_batch<batch_num>
        prepare_ligands = KubernetesPodOperator(
            task_id='prepare_ligands',
            full_pod_spec=full_pod_spec,
            cmds=get_prepare_ligands_cmd(batch_label),
            get_logs=True,
        )

        @task 
        def get_perform_docking_cmd(batch_label, params=None):
            return ['/bin/sh', '-c', f'echo "perform_docking({ params["pdbid"] }, { batch_label })"; sleep 6']

        # perform_docking: <filelist> -> ()
        perform_docking = KubernetesPodOperator(
            task_id='perform_docking',
            full_pod_spec=full_pod_spec_gpu,
            container_resources=k8s.V1ResourceRequirements(
                limits={"nvidia.com/gpu": "1"}
            ),
            pool='gpu_pool',

            cmds=get_perform_docking_cmd(batch_label),
            get_logs=True # otherwise generates too much log
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