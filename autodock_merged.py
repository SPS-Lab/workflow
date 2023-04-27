from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.configuration import conf
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

import os
import os.path
import time
from datetime import datetime

from kubernetes.client import models as k8s

PVC_NAME = 'pvc-autodock'
MOUNT_PATH = '/data'
VOLUME_KEY  = 'volume-autodock'

# Parameters 
# TODO: replace with DAG parameters
AUTOGRID_GRID_CENTER = (49.8363, 17.6087, 36.2723)

namespace = conf.get('kubernetes_executor', 'NAMESPACE')

params = {
    # PDBID of the receptor, which will be used to fetch protein data from PDB
    'pdbid': '7cpa',

    # label of the ligand database, the filename for 
    # the corresponding database must be '{db_label}.sdf'
    'ligand_db': 'sweetlead'
}


def preprocess_input(**context):
    list_of_inputs = ["A", "B", "C", "D", "E"]
    Variable.set(key="list_of_inputs", value=list_of_inputs, serialize_json=True)

with DAG(dag_id="autodock_full", 
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False, params=params) as dag:

    volume = k8s.V1Volume(
        name=VOLUME_KEY,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
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

    # CPU/generic pod specification
    pod_spec      = k8s.V1PodSpec(containers=[container], volumes=[volume])
    full_pod_spec = k8s.V1Pod(spec=pod_spec)

    # GPU-specific pod specification
    pod_spec_gpu      = k8s.V1PodSpec(containers=[container], volumes=[volume], runtime_class_name='nvidia')
    full_pod_spec_gpu = k8s.V1Pod(spec=pod_spec_gpu)

    # 1b - Prepare the ligands
    prepare_ligands = KubernetesPodOperator(
        task_id='prepare_ligands',
        full_pod_spec=full_pod_spec,

        cmds=['/autodock/scripts/1b_prepare_ligands.sh', '{{ params.pdbid }}', '{{ params.ligand_db }}'],
    )

    prepare_input = PythonOperator(task_id='prepare_input', python_callable=preprocess_input, dag=dag)

    json_table = Variable.get("list_of_inputs", deserialize_json=True, default_var=None)
    #table_list = json_table["list_of_inputs"]

    with TaskGroup("taskgroup_1", tooltip="task group #1") as task_parallel:
        for input in json_table:
            # 1a - Prepare the protein
            prepare_receptor = KubernetesPodOperator(
                task_id='prepare_receptor' + input,
                full_pod_spec=full_pod_spec,
                cmds = ['/usr/bin/sleep', '10']
                #cmds=['/autodock/scripts/1a_fetch_prepare_protein.sh', '{{ params.pdbid }}'],
            )

            # 2 - Perform docking
            docking = KubernetesPodOperator(
                task_id='docking' + input,
                full_pod_spec=full_pod_spec_gpu,
                #container_resources=k8s.V1ResourceRequirements(
                #    limits={"nvidia.com/gpu": "1"}
                #),
                cmds = ['/usr/bin/sleep', '10']
                #cmds=['/autodock/scripts/2_docking.sh', '{{ params.pdbid }}', '{{ params.ligand_db }}'],
                # get_logs=False # otherwise generates too much log
            )
            prepare_receptor >> docking

        # 3 - Post-processing (extracting relevant data)
    postprocessing = KubernetesPodOperator(
        task_id='postprocessing',
        full_pod_spec=full_pod_spec,
        cmds = ['/usr/bin/sleep', '10']
            #cmds=['/autodock/scripts/3_post_processing.sh', '{{ params.pdbid }}', '{{ params.ligand_db }}'],
        )

    emptyop = EmptyOperator(task_id="end")
    
    [prepare_ligands, prepare_input] >> task_parallel >> postprocessing >> emptyop

#prep_dock()