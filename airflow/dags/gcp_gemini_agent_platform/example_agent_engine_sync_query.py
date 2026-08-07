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
"""Exercise a synchronous Vertex AI Reasoning Engine query end to end."""

from __future__ import annotations

from datetime import datetime

from airflow.providers.google.cloud.operators.vertex_ai.agent_engine import (
    CreateAgentEngineOperator,
    DeleteAgentEngineOperator,
    GetAgentEngineOperator,
    RunReasoningEngineQueryOperator,
)

try:
    from airflow.sdk import DAG, TriggerRule
except ImportError:
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]


DAG_ID = "vertex_ai_agent_engine_sync_query_e2e"
PROJECT_ID = "{{ var.value.GCP_PROJECT_ID }}"
LOCATION = "{{ var.value.get('GCP_REGION', 'us-central1') }}"
CONTAINER_URI = "{{ var.value.GCP_AGENT_ENGINE_CONTAINER_URI }}"
AGENT_ENGINE_ID = "{{ ti.xcom_pull(task_ids='create_agent_engine')['name'].split('/')[-1] }}"


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["e2e", "vertex_ai", "agent_engine", "reasoning_engine"],
) as dag:
    create_agent_engine = CreateAgentEngineOperator(
        task_id="create_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        config={
            "display_name": "airflow-sync-query-e2e",
            "description": "Airflow synchronous Reasoning Engine query E2E",
            "agent_framework": "custom",
            "min_instances": 0,
            "max_instances": 1,
            "resource_limits": {"cpu": "1", "memory": "1Gi"},
            "container_spec": {"image_uri": CONTAINER_URI},
            "class_methods": [{"name": "query", "api_mode": ""}],
        },
    )

    get_agent_engine = GetAgentEngineOperator(
        task_id="get_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
    )

    run_reasoning_engine_query = RunReasoningEngineQueryOperator(
        task_id="run_reasoning_engine_query",
        project_id=PROJECT_ID,
        location=LOCATION,
        reasoning_engine_id=AGENT_ENGINE_ID,
        input_data={"input": "hello from Airflow E2E"},
        timeout=300,
    )

    delete_agent_engine = DeleteAgentEngineOperator(
        task_id="delete_agent_engine",
        project_id=PROJECT_ID,
        location=LOCATION,
        agent_engine_id=AGENT_ENGINE_ID,
        force=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_agent_engine >> get_agent_engine >> run_reasoning_engine_query >> delete_agent_engine
