from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.configuration import conf

from datetime import datetime

default_args = {}
namespace = conf.get('kubernetes', 'NAMESPACE')

@task
def detect_receptor_types():
    pass

@task
def prepare_grid():
    return None

@dag(start_date=datetime(2021, 1, 1),
     schedule=None,
     catchup=False,
     default_args=default_args)
def autogrid():
    prepare_grid()

autogrid()
