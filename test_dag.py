from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task

from datetime import datetime

@dag(start_date=datetime(2021, 1, 1), 
    schedule=None)
def test_dag(): 
    prepare_receptor = BashOperator(
        task_id='prepare_receptor',
        bash_command='echo "I am task prepare_receptor" && sleep 1'
    )

    prepare_ligands = BashOperator(
        task_id='prepare_ligands',
        bash_command='echo "I am task prepare_ligands" && sleep 2'
    )

    cmd = '|'.join([
        'echo a b c',
        r'xargs -0 printf \"%s\",',
        r'sed "s/^\(.*\).$/[\1]/" > /airflow/xcom/return.json'
    ])

    split_sdf = BashOperator(
        task_id='split_sdf',
        bash_command=cmd,
        do_xcom_push=True
    )

    @task
    def docking(pdbid: str, batch_fname: str):
        print(f'Docking - PBDID: {pdbid}, batch_fname: {batch_fname}')

    docking_tasks = docking.partial(pdbid='7cpa').expand(batch_fname=['batch0', 'batch1', 'batch3', 'batch4'])

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing" && sleep 1'
    )

    split_sdf >> prepare_ligands

    [prepare_receptor, prepare_ligands] >> docking_tasks >> postprocessing

test_dag()
