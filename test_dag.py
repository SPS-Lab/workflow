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

    @task
    def docking(x: int):
        print(f'Bara bra #{x}')

    docking_tasks = docking.expand(x=[1,2,3,4,5,6])

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing" && sleep 1'
    )

    [prepare_receptor, prepare_ligands] >> docking_tasks >> postprocessing

test_dag()
