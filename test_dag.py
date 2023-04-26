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

    ts = []
    for i in range(5):
        t = BashOperator(
            task_id=f'docking_{i}',
            bash_command=f'echo "I am task docking #{i}" && sleep 5'
        )
        ts.append(t)

    postprocessing = BashOperator(
        task_id='postprocessing',
        bash_command='echo "I am task postprocessing" && sleep 1'
    )

    [prepare_receptor, prepare_ligands] >> ts >> postprocessing

test_dag()
