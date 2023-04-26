from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task

from datetime import datetime

@dag(start_date=datetime(2021, 1, 1), 
    schedule=None)
def test_dag(): 

    prepare_receptor = BashOperator(
        task_id='prepare_receptor',
        bash_command='echo "I am task prepare_receptor"'
    )

    prepare_ligands = BashOperator(
        task_id='prepare_ligands',
        bash_command='echo "I am task prepare_ligands"'
    )

    docking = BashOperator(
        task_id='docking',
        bash_command='echo "I am task docking"'
    )

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing"'
    )

    [prepare_receptor, prepare_ligands] >> docking >> postprocessing

test_dag()
