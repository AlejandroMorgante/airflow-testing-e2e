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
"""Edit a SQL warehouse setting for manual end-to-end UI validation."""

from __future__ import annotations

from airflow.providers.databricks.operators.warehouse import DatabricksEditWarehouseOperator
from airflow.sdk import DAG, Param

with DAG(
    dag_id="databricks_warehouse_edit_e2e",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "warehouse_id": Param("", type="string", minLength=1),
        "auto_stop_mins": Param(20, type="integer", minimum=10),
    },
    tags=["e2e", "databricks", "pr-76504"],
) as dag:
    DatabricksEditWarehouseOperator(
        task_id="edit_warehouse",
        databricks_conn_id="databricks_oauth",
        warehouse_id="{{ params.warehouse_id }}",
        json={"auto_stop_mins": "{{ params.auto_stop_mins }}"},
    )
