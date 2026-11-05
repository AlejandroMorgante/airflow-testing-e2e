# Databricks warehouse edit end-to-end test

Validate Apache Airflow PR https://github.com/apache/airflow/pull/76504 against a real SQL warehouse using Airflow 3.3.0, Python 3.12, Postgres 16 and LocalExecutor. Mount the provider source from the PR checkout; the installed wheel alone does not contain this operator.

The manually triggered `databricks_warehouse_edit_e2e` Dag contains one `DatabricksEditWarehouseOperator`. Supply `warehouse_id` and `auto_stop_mins` (default 20) in the trigger form. Record the original setting in the Databricks UI before running. After inspecting the change, trigger the same Dag with the original value to restore it. No start or stop operation is requested.

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

Open http://localhost:8084 and log in with the local-only `admin` / `admin` account. Unpause the Dag and trigger it with the selected `warehouse_id` and `auto_stop_mins`. Inspect the rendered request and task logs, then confirm the new value in the Databricks UI. Trigger another run with the original value and confirm restoration there. A green task confirms that the edit request succeeded; remote settings are verified separately.

Capture the Airflow graph and edit logs, and the Databricks warehouse settings UI showing the changed value before restoration. Capture the restored setting afterward. Record the tested provider commit alongside the evidence.

Stop the local stack after review:

```sh
docker compose --env-file /path/to/runtime.env -f airflow/docker-compose.databricks-warehouse.yml down
```

Volumes retain the run history for later review. No live-environment success is claimed until the Dag completes.

## Resize followed by a query

Set `DATABRICKS_WAREHOUSE_ID` in the private environment file. The `databricks_warehouse_resize_query_e2e` Dag contains exactly two operators:

```text
DatabricksEditWarehouseOperator >> DatabricksSQLStatementsOperator
```

Record the current size and start the test warehouse before triggering this scenario, so the run exercises resizing active compute. Select a different `cluster_size` in the trigger form (default X-Small). The edit task only submits the configuration change. The SQL task immediately submits `SELECT 1 AS resize_pipeline_probe` with `wait_for_termination=True`, polls every second, and has a 180-second statement polling timeout plus a four-minute task execution timeout. It uses synchronous execution so the Airflow task remains running while Databricks reports a pending/running statement. No custom waiter or Python task is used.

Review `select_one` logs and its `statement_id` XCom against Databricks Query History. A resize can finish before the scheduler starts the downstream task, so seeing PENDING is timing-dependent; a successful Dag alone does not prove a pending phase occurred. Record the actual observations. After review, restore the original warehouse size and stop the warehouse to avoid idle charges.

### Observed run (2026-11-05 UTC)

With provider source commit `1285e05c394a375c291e78de7bf005c231bafb41`, `manual__resize_then_select_20261105` successfully resized an active serverless warehouse from Small to X-Small and executed the downstream statement. Both tasks succeeded. Query History reported one output row and 4,978 ms total query time. Its provisioning-queue and compilation-start timestamps were 1,821 ms apart. The synchronous operator remained active until statement completion, although its logs did not capture a PENDING poll in this run. This validates this operator/API path; other SQL clients may have different waiting and timeout behavior.
