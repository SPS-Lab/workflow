#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
### DAG Tutorial Documentation
This DAG is demonstrating an Extract -> Transform -> Load pipeline
"""
from __future__ import annotations

# [START tutorial]
# [START import_module]
import json
from textwrap import dedent

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]

# [START instantiate_dag]
with DAG(
    "AutoDock_dag",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 2},
    # [END default_args]
    description="AutoDock",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["autodock"],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START setup_function]
    def setup(**kwargs):
        ti = kwargs["ti"]
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        ti.xcom_push("start_preprocessing_PDB", data_string)

    # [START processligand_function]
    def process_ligand(**kwargs):
        ti = kwargs["ti"]
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        ti.xcom_push("processligand_PDB", data_string)

    # [START processgrid_function]
    def process_grid(**kwargs):
        ti = kwargs["ti"]
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        ti.xcom_push("processgrid_PDB", data_string)

    # [END extract_function]

    # [START transfer_ligand_to_GPU]
    def transfer_ligand(**kwargs):
        ti = kwargs["ti"]
        extract_data_string = ti.xcom_pull(task_ids="processligand_PDB", key="order_data")
        order_data = json.loads(extract_data_string)

        total_order_value = 0
        for value in order_data.values():
            total_order_value += value

        total_value = {"total_order_value": total_order_value}
        total_value_json_string = json.dumps(total_value)
        ti.xcom_push("transfer_ligand_PDB", total_value_json_string)

    # [END transfer_ligand_to_GPU]

    # [START transfer_grid_to_GPU]
    def transfer_grid(**kwargs):
        ti = kwargs["ti"]
        extract_data_string = ti.xcom_pull(task_ids="processgrid_PDB", key="order_data")
        order_data = json.loads(extract_data_string)

        total_order_value = 0
        for value in order_data.values():
            total_order_value += value

        total_value = {"total_order_value": total_order_value}
        total_value_json_string = json.dumps(total_value)
        ti.xcom_push("transfer_grid_PDB", total_value_json_string)

    # [END transfer_ligand_to_GPU]

    # [START load_function]
    def scoring_function(**kwargs):
        ti = kwargs["ti"]
        total_value_string1 = ti.xcom_pull(task_ids="transfer_ligand_PDB", key="total_order_value")
        total_value_string2 = ti.xcom_pull(task_ids="transfer_grid_PDB", key="total_order_value")
        total_order_value = json.loads(total_value_string)

        print(total_order_value)
        ti.xcom_push("docking_PDB", total_value_json_string)

    # [END scoring_function]

    # [START processing_results_function]
    def process_results_function(**kwargs):
        ti = kwargs["ti"]
        total_value_string = ti.xcom_pull(task_ids="docking_PDB", key="total_order_value")
        total_order_value = json.loads(total_value_string)

        print(total_order_value)

    # [END scoring_function]

    # [START main_flow]
    setup_task = PythonOperator(
        task_id="setup",
        python_callable=setup,
    )
    setup_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    process_ligand_task = PythonOperator(
        task_id="processligand",
        python_callable=process_ligand,
    )
    process_ligand_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )


    process_grid_task = PythonOperator(
        task_id="processgrid",
        python_callable=process_grid,
    )
    process_grid_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )


    transfer_ligand_task = PythonOperator(
        task_id="transferligand",
        python_callable=transfer_ligand,
    )
    transfer_ligand_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )


    transfer_grid_task = PythonOperator(
        task_id="transfergrid",
        python_callable=transfer_grid,
    )
    transfer_grid_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    docking_task = PythonOperator(
        task_id="docking",
        python_callable=scoring_function,
    )
    docking_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    processing_results_task = PythonOperator(
        task_id="process_results",
        python_callable=process_results_function,
    )
    docking_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    setup_task >> process_ligand_task >> transfer_ligand_task >> docking_task >> processing_results_task
    setup_task >> process_grid_task   >> transfer_grid_task   >> docking_task >> processing_results_task

# [END main_flow]

# [END tutorial]
