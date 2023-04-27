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

    @task
    def prepare_ligands(fname_sdf: str):
        return KubernetesPodOperator(
            task_id='prepare_ligands',
            namespace=namespace,
            image="alpine",
            cmds=["sh", "-c", f"echo '[\"barabra {fname_sdf}\"]' "],
            do_xcom_push=True
        )
    
    @task
    def docking(batch_fname: str):
        print(f'Docking - batch_fname: {batch_fname}')

    prepared = prepare_ligands.expand(fname_sdf=split_sdf.output)
    docking.expand(batch_fname=prepared)

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing" && sleep 1'
    )

test_dag()
