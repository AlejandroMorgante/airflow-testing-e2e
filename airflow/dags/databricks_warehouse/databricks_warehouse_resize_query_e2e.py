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
"""Resize a warehouse and let the SQL statement operator wait for its query."""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.providers.databricks.operators.databricks import DatabricksSQLStatementsOperator
from airflow.providers.databricks.operators.warehouse import DatabricksEditWarehouseOperator
from airflow.sdk import DAG, Param

WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

with DAG(
    dag_id="databricks_warehouse_resize_query_e2e",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={"cluster_size": Param("X-Small", enum=["2X-Small", "X-Small", "Small"])},
    tags=["e2e", "databricks", "pr-76504"],
) as dag:
    resize = DatabricksEditWarehouseOperator(
        task_id="resize_warehouse",
        databricks_conn_id="databricks_oauth",
        warehouse_id=WAREHOUSE_ID,
        json={"cluster_size": "{{ params.cluster_size }}"},
        execution_timeout=timedelta(minutes=5),
    )
    query = DatabricksSQLStatementsOperator(
        task_id="select_one",
        databricks_conn_id="databricks_oauth",
        warehouse_id=WAREHOUSE_ID,
        statement="SELECT 1 AS resize_pipeline_probe",
        wait_for_termination=True,
        polling_period_seconds=1,
        timeout=180,
        execution_timeout=timedelta(minutes=4),
        deferrable=False,
    )
    resize >> query
