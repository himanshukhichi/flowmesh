# FlowMesh

Distributed job orchestration engine that executes dependency-aware DAGs across a horizontally scalable worker cluster.

## Core slice

This first implementation includes:

- Spring Boot 3 service scaffold with PostgreSQL/Flyway persistence.
- JSON and YAML DAG submission.
- DAG validation with duplicate task checks, unknown dependency checks, DFS cycle detection, and Kahn topological sorting.
- Immutable DAG versioning: every submission creates a new `{dagId, version}` record.
- DAG run creation tied to a specific version, with root tasks moved to `PENDING`.
- A database-backed scheduling tick that locks ready `PENDING` tasks and marks them `QUEUED`.
- Local Docker Compose services for PostgreSQL, Redis, Kafka, and Prometheus.

## DAG schema

```json
{
  "dagId": "daily-pipeline",
  "name": "Daily Pipeline",
  "tasks": [
    {
      "taskId": "extract",
      "type": "http_call",
      "dependsOn": [],
      "config": {"url": "https://example.test/source"},
      "timeoutSecs": 60,
      "retries": 3
    },
    {
      "taskId": "load",
      "type": "sql_query",
      "dependsOn": ["extract"],
      "config": {"sql": "select 1"},
      "timeoutSecs": 120,
      "retries": 3
    }
  ]
}
```

Cycle submissions are rejected with:

```json
{
  "error": "CYCLE_DETECTED",
  "path": ["taskA", "taskB", "taskA"]
}
```

## Run locally

Start infrastructure:

```bash
docker compose up -d postgres redis kafka prometheus
```

Run the service:

```bash
mvn spring-boot:run
```

Submit a DAG:

```bash
curl -X POST http://localhost:8080/api/dags \
  -H 'Content-Type: application/json' \
  -d @examples/dag.json
```

Create a run from the latest version:

```bash
curl -X POST http://localhost:8080/api/dags/daily-pipeline/runs
```

Inspect the latest immutable version:

```bash
curl http://localhost:8080/api/dags/daily-pipeline/versions/latest
```

Run tests:

```bash
mvn test
```
