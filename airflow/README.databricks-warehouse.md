# Databricks warehouse resize and query test

Reproduce the real-environment validation of [apache/airflow#76504](https://github.com/apache/airflow/pull/76504) with Airflow 3.3.0, Python 3.12, Postgres 16 and LocalExecutor. The `databricks_warehouse_resize_query_e2e` Dag uses two operators:

```text
DatabricksEditWarehouseOperator >> DatabricksSQLStatementsOperator
```

The edit request changes `cluster_size`; the downstream operator submits `SELECT 1 AS resize_pipeline_probe` and waits for statement completion. There is no separate warehouse waiter.

## Run

1. Check out the Airflow PR source (validated commit `1285e05c394a375c291e78de7bf005c231bafb41`).
2. Copy `airflow/databricks-warehouse.env.example` to a private file outside the repository. Set the Airflow checkout path, warehouse ID, a random JWT secret, and the path to a private connections JSON file:

   ```json
   {"databricks_oauth":{"conn_type":"databricks","host":"https://YOUR-WORKSPACE.cloud.databricks.com","password":"YOUR-ACCESS-TOKEN"}}
   ```

   Use a fresh short-lived token. Keep both files outside Git and make the connections file readable by the container user.
3. From the repository root, build and start the local environment:

   ```sh
   docker build -f airflow/Dockerfile.databricks-warehouse -t airflow-databricks-agent-e2e:latest airflow
   docker compose --env-file /path/to/runtime.env -f airflow/docker-compose.databricks-warehouse.yml up -d --no-build
   ```

4. Record the warehouse's original size and start it. Open http://localhost:8084 (`admin` / `admin`), unpause `databricks_warehouse_resize_query_e2e`, and trigger it with a different `cluster_size` (default `X-Small`).
5. Inspect the rendered edit request and both task logs. Match the SQL statement ID with Databricks Query History to inspect compute waiting and execution. A successful Dag alone does not prove a pending phase occurred.
6. Restore the original warehouse size, stop the warehouse, and stop the local stack:

   ```sh
   docker compose --env-file /path/to/runtime.env -f airflow/docker-compose.databricks-warehouse.yml down
   ```

## Observed result

Run `manual__resize_then_select_20261105` changed Small to X-Small and completed both tasks successfully. Query History reported one output row, 4.978 seconds total duration, and 1.170 seconds waiting for compute. The SQL operator waited for completion, although its logs did not capture a PENDING poll. Screenshots are in the [Airflow PR](https://github.com/apache/airflow/pull/76504). The original size was restored and the warehouse stopped afterward.
