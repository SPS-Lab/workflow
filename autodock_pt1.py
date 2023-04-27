from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.configuration import conf
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
import os
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

def preprocess_input(context, dag_run_obj):
    list_of_inputs = ["A", "B", "C", "D"]
    Variable.set(key="list_of_inputs", value=list_of_inputs, serialize_json=True)
    return True 

@dag(start_date=datetime(2021, 1, 1),
     schedule=None,
     catchup=False,
     dag_id="autodock_pt1", params=params)
def prep_dock():
    import os.path

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

    prepare_input = BashOperator(
        task_id="prepare_input",
        bash_command="echo 1",
    )

    start_ligand_and_docking = TriggerDagRunOperator(
        task_id="autodock_pt2",
        trigger_dag_id="docking",
        )

    emptyop = EmptyOperator(task_id="end")
    
    [prepare_ligands, prepare_input] >> start_ligand_and_docking >> emptyop

prep_dock()