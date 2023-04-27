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

namespace = conf.get('kubernetes_executor', 'NAMESPACE')

@dag(start_date=datetime(2021, 1, 1),
     schedule=None,
     catchup=False,
     params=params, dag_id="autodock_pt2.py")
def autodock(): 
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

    json_table = Variable.get("dag_update_data_var", deserialize_json=True, default_var={"list_of_inputs":["A"]})
    table_list = json_table["list_of_inputs"]

    for input in table_list:

        # 1a - Prepare the protein
        prepare_receptor = KubernetesPodOperator(
            task_id='prepare_receptor',
            full_pod_spec=full_pod_spec,
            cmds = ['/usr/bin/sleep', '10']
            #cmds=['/autodock/scripts/1a_fetch_prepare_protein.sh', '{{ params.pdbid }}'],
        )

        # 2 - Perform docking
        docking = KubernetesPodOperator(
            task_id='docking',
            full_pod_spec=full_pod_spec_gpu,
            container_resources=k8s.V1ResourceRequirements(
                limits={"nvidia.com/gpu": "1"}
            ),
            cmds = ['/usr/bin/sleep', '10']
            #cmds=['/autodock/scripts/2_docking.sh', '{{ params.pdbid }}', '{{ params.ligand_db }}'],
            # get_logs=False # otherwise generates too much log
        )

        # 3 - Post-processing (extracting relevant data)
        postprocessing = KubernetesPodOperator(
            task_id='postprocessing',
            full_pod_spec=full_pod_spec,
            cmds = ['/usr/bin/sleep', '10']
            #cmds=['/autodock/scripts/3_post_processing.sh', '{{ params.pdbid }}', '{{ params.ligand_db }}'],
        )

        prepare_receptor >> docking
    docking >> postprocessing

autodock()