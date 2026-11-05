# Databricks warehouse edit end-to-end test

Validate Apache Airflow PR https://github.com/apache/airflow/pull/76504 against a real SQL warehouse using Airflow 3.3.0, Python 3.12, Postgres 16 and LocalExecutor. Mount the provider source from the PR checkout; the installed wheel alone does not contain this operator.

The manually triggered `databricks_warehouse_edit_e2e` Dag reads the current `auto_stop_mins`, changes it to 20 when the original is 10 (otherwise to 10), verifies the new value through the API, restores the original value, and verifies restoration. Cleanup runs even if verification fails. If snapshot or restoration fails, inspect the task logs and restore the saved original value manually. No start or stop operation is requested.

## Setup

Build once, or reuse the existing image from the Databricks agent scenario:

```sh
docker build -f airflow/Dockerfile.databricks-warehouse -t airflow-databricks-agent-e2e:latest airflow
```

Copy `databricks-warehouse.env.example` to a private file outside the repository. Set the checkout path, a random JWT secret, and a private Airflow connections JSON file:

```json
{"databricks_oauth":{"conn_type":"databricks","host":"https://YOUR-WORKSPACE.cloud.databricks.com","extra":{"token":"YOUR-ACCESS-TOKEN"}}}
```

Use a fresh short-lived access token; renew it before rerunning after expiration. Never commit the connection file or runtime environment. Keep the parent directory private and make the mounted connection file readable by the container user.

```sh
docker compose --env-file /path/to/runtime.env -f airflow/docker-compose.databricks-warehouse.yml up -d --no-build
```

Open http://localhost:8084 and log in with the local-only `admin` / `admin` account. Unpause the Dag and trigger it with the selected `warehouse_id`. The UI shows the rendered request, verification outputs and logs. A successful run has five successful tasks and restores the original setting.

Capture the Airflow graph, edit and verification logs, and the Databricks warehouse settings UI. The final Databricks settings show the restored value; the task logs record the temporary value verified through the real API. Record the tested provider commit alongside the evidence.

Stop the local stack after review:

```sh
docker compose --env-file /path/to/runtime.env -f airflow/docker-compose.databricks-warehouse.yml down
```

Volumes retain the run history for later review. No live-environment success is claimed until the Dag completes.
