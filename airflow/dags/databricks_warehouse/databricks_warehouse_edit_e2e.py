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
"""Verify warehouse edits and restore the original auto-stop setting."""

from __future__ import annotations

from datetime import timedelta

import structlog

from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.warehouse import DatabricksEditWarehouseOperator
from airflow.sdk import DAG, Param, task

log = structlog.get_logger()

CONN_ID = "databricks_oauth"

with DAG(
    dag_id="databricks_warehouse_edit_e2e",
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={"warehouse_id": Param("", type="string", minLength=1)},
    tags=["e2e", "databricks", "pr-76504"],
) as dag:

    @task
    def snapshot(warehouse_id: str) -> dict:
        warehouse = DatabricksHook(CONN_ID).get_warehouse(warehouse_id)
        original = warehouse["auto_stop_mins"]
        if not isinstance(original, int):
            raise ValueError("Expected an integer auto_stop_mins before editing")
        target = 20 if original == 10 else 10
        log.info("warehouse_settings_snapshot", original=original, target=target)
        return {"warehouse_id": warehouse_id, "original": original, "target": target}

    @task(retries=5, retry_delay=timedelta(seconds=10))
    def verify(settings: dict, expected_key: str) -> dict:
        warehouse = DatabricksHook(CONN_ID).get_warehouse(settings["warehouse_id"])
        actual = warehouse["auto_stop_mins"]
        expected = settings[expected_key]
        if actual != expected:
            raise ValueError(f"Expected auto_stop_mins={expected}, received {actual}")
        log.info("warehouse_setting_verified", auto_stop_mins=actual)
        return {"verified": True, "auto_stop_mins": actual}

    settings = snapshot("{{ params.warehouse_id }}")
    edit = DatabricksEditWarehouseOperator(
        task_id="edit_warehouse",
        databricks_conn_id=CONN_ID,
        warehouse_id="{{ params.warehouse_id }}",
        json={"auto_stop_mins": "{{ ti.xcom_pull(task_ids='snapshot')['target'] }}"},
    )
    verified = verify.override(task_id="verify_edit")(settings, "target")
    restore = DatabricksEditWarehouseOperator(
        task_id="restore_original_setting",
        databricks_conn_id=CONN_ID,
        warehouse_id="{{ params.warehouse_id }}",
        json={"auto_stop_mins": "{{ ti.xcom_pull(task_ids='snapshot')['original'] }}"},
        trigger_rule="all_done",
    )
    restored = verify.override(task_id="verify_restoration")(settings, "original")
    settings >> edit >> verified >> restore >> restored
