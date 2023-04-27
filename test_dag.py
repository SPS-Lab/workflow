from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task

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
        'echo a b c',
        r'xargs printf \"%s\",',
        r'sed "s/^\(.*\).$/[\1]/" > /airflow/xcom/return.json'
    ])
    split_sdf = KubernetesPodOperator(
        namespace=namespace,
        task_id='split_sdf',
        image="alpine",
        cmds=["sh", "-c", cmd],
        do_xcom_push=True,
    )

    """prepare_ligands = PrepareLigandOperator.partial(
        task_id='prepare_ligands',
        do_xcom_push=True
    ).expand(sdf_name=split_sdf)"""

    @task
    def prepare_ligands(sdf_name: str):
        print(f'sdf_name={sdf_name}')

        return sdf_name + '_prepared'

    @task
    def docking(batch_fname: str):
        print(f'Docking - batch_fname: {batch_fname}')


    prepared = prepare_ligands.expand(sdf_name=split_sdf.output)
    docked = docking(prepared)

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing" && sleep 1'
    )

    prepare_receptor >> docked >> postprocessing

test_dag()
