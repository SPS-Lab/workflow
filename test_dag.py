from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task

from datetime import datetime

from airflow.configuration import conf
from airflow import XComArg
from airflow.models.xcom_arg import MapXComArg

namespace = conf.get('kubernetes_executor', 'NAMESPACE')

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

    def barabra(f):
        return [f]

    prepare_ligands = KubernetesPodOperator.partial(
        namespace=namespace,
        task_id='prepare_ligands',
        image="alpine",
        cmds=["sh", "-c"],
    ).expand(arguments=split_sdf.output.map(barabra))

    @task
    def docking(pdbid: str, batch_fname: str):
        print(f'Docking - PBDID: {pdbid}, batch_fname: {batch_fname}')

    docking_tasks = docking.partial(pdbid='7cpa').expand(batch_fname=['a', 'b'])

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing" && sleep 1'
    )

    [prepare_receptor, prepare_ligands] >> docking_tasks >> postprocessing

test_dag()
