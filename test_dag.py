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

    split_sdf = KubernetesPodOperator(
        namespace=namespace,
        task_id='split_sdf',
        image="alpine",
        cmds=["sh", "-c", 'echo [\\"a\\", \\"b\\", \\"c\\"] > /airflow/xcom/return.json'],
        do_xcom_push=True,
    )

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing" && sleep 1'
    )

    @task
    def get_batch_labels(db_label: str, n: int):
        return [f'{db_label}_batch{i}' for i in range(n)]

    @task_group
    def docking(batch_label: str):

        @task
        def t(x): 
            return [f'echo \\"coucou_{x}\\" > /airflow/xcom/return.json']

        x = t(batch_label)

        prepare_ligands = KubernetesPodOperator(
            task_id='prepare_ligands',
            namespace=namespace,
            image='alpine',
            cmds=['sh', '-c'],
            arguments=x,
            get_logs=True,
        )

        perform_docking = KubernetesPodOperator(
            task_id='perform_docking',
            namespace=namespace,
            image='alpine',
            cmds=['sh', '-c'],
            arguments=[f'echo {prepare_ligands.output}']
        )

        x >> prepare_ligands >> perform_docking

    docking.expand(batch_label=split_sdf.output) >> postprocessing


test_dag()
    
"""# Convert 'N batches' to [batch1, batch2, ... batchN]
@task
def get_batch_labels(db_label: str, n: int):
    return [f'{db_label}_batch{i}' for i in range(n)]"""