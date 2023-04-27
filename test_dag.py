from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task, task_group

from datetime import datetime
import random

from airflow.configuration import conf
from airflow import XComArg
from airflow.models.xcom_arg import MapXComArg

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

@dag(start_date=datetime(2021, 1, 1), 
    schedule=None)
def test_dag(): 
    prepare_receptor = BashOperator(
        task_id='prepare_receptor',
        bash_command='echo "I am task prepare_receptor" && sleep 1'
    )

    # converts the returned value to a JSON string
    cmd = '|'.join([
        'echo 10 > /airflow/xcom/return.json'
    ])
    split_sdf = KubernetesPodOperator(
        namespace=namespace,
        task_id='split_sdf',
        image="alpine",
        cmds=["sh", "-c", cmd],
        do_xcom_push=True,
    )

    """
    prepare_ligands = KubernetesPodOperator(
        namespace=namespace,
        task_id='prepare_ligands',
        image="alpine",
        cmds=["sh", "-c", f'echo prepare_ligands: {sdf_name}!; echo \\"{sdf_name}\\" | tee /airflow/xcom/return.json'],
        do_xcom_push=True,
    )"""

    @task_group
    def docking(batch_label: str):
        @task
        def prepare_ligands(sdf_name: str):
            print(f'prepare {sdf_name}')
            return sdf_name + '_prepared'

        @task
        def perform_docking(batch_fname: str):
            print(f'docking: {batch_fname}')
        
        perform_docking(prepare_ligands(batch_label))

    @task
    def get_batch_labels(n:int):
        return [f'barabra_batch{i}.sdf' for i in range(n)]

    batch_labels = get_batch_labels(split_sdf.output)

    docked = docking.expand(batch_label=batch_labels)

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing" && sleep 1'
    )

    prepare_receptor >> docked >> postprocessing

test_dag()
