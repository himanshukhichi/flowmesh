# FlowMesh

Distributed job orchestration engine that executes dependency-aware DAGs across a horizontally scalable worker cluster.

## Stack

- Java 21, Spring Boot 3, PostgreSQL, Flyway, Apache Kafka, Redis, gRPC, Prometheus, JUnit 5, Docker Compose, AWS ECS/Fargate scaffold.

## Feature status

| Area | Status | Implementation |
| --- | --- | --- |
| DAG JSON/YAML schema | Complete | `dagId`, `name`, `tasks`, `dependsOn`, `config`, `timeoutSecs`, `retries`, `success_branch`, `failure_branch`. |
| Cycle detection | Complete | DFS rejects cycles with `{error: "CYCLE_DETECTED", path: [...]}`. |
| Topological sort | Complete | Kahn ordering plus initial ready task discovery. |
| DAG versioning | Complete | Every submission creates an immutable PostgreSQL version; runs pin one version. |
| Conditional branching | Complete | Success/failure branch targets are validated and respected by dependency readiness queries. |
| Redis leader election | Complete | Scheduler instances contend for a Redis TTL lock and renew while active. |
| Scheduling loop | Complete | Leader polls PostgreSQL, row-locks ready tasks, queues to Kafka, and transitions state. |
| Task state machine | Complete | State transitions are persisted to `task_state_transitions` for timeline/audit. |
| Timeout and heartbeat expiry | Complete | Leader scans locked running tasks and routes failures through retry/DLQ logic. |
| Pause/resume | Complete | Redis-backed global and per-DAG admin controls. |
| Kafka task distribution | Complete | Topic per task type plus retry and DLQ topics. |
| Transactional Kafka dispatch | Complete | PostgreSQL outbox rows commit with task state; a Kafka transactional publisher drains them with `read_committed` consumers. |
| Idempotency dedupe | Complete | Workers reserve `dagRunId + taskId` keys before handler execution and reject stale attempts. |
| DLQ and requeue | Complete | DLQ consumer persists context; admin API lists and requeues tasks. |
| Worker nodes | Complete | Kafka consumer workers with pluggable handlers for `http_call`, `sql_query`, and `ml_inference`. |
| Retry backoff | Complete | Worker/scheduler failure path uses 1s, 2s, 4s style exponential retry delays, then DLQ. |
| Worker gRPC registration | Complete | Workers register supported task types/concurrency and send heartbeats every 10s. |
| Horizontal scaling demo | Complete | Compose defines 1 scheduler, 2 standbys, and 5 workers; load generator emits a root-plus-fanout 1,000-task DAG for parallel consumption. |
| Prometheus metrics | Complete | Scheduler, retry, DLQ, Kafka queue depth, leader election, execution latency, and scheduling latency metrics. |
| MDC trace IDs | Complete | Logs include `dagRunId`, `taskId`, and `workerId`. |
| Run timeline API | Complete | `GET /api/runs/{dagRunId}/timeline` returns state transition events. |
| AWS ECS deploy | Complete | Fargate CloudFormation scaffold in `deploy/ecs/flowmesh-ecs.yml`. |

Kafka dispatch uses a PostgreSQL outbox. Scheduler and worker state changes commit in the same database transaction as the outbound Kafka payload, then `KafkaOutboxPublisher` drains pending rows with the Kafka transactional producer. Consumers use `read_committed`. If a publisher crashes after Kafka commit but before marking the outbox row sent, the row can be retried; worker idempotency keys and task state checks prevent duplicate execution. This avoids cross-resource XA while keeping the demo stack crash-tolerant.

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
      "retries": 3,
      "success_branch": "load",
      "failure_branch": "notify"
    },
    {
      "taskId": "load",
      "type": "sql_query",
      "dependsOn": ["extract"],
      "config": {"sql": "select 1"},
      "timeoutSecs": 120,
      "retries": 3
    },
    {
      "taskId": "notify",
      "type": "http_call",
      "dependsOn": ["extract"],
      "config": {"url": "https://example.test/notify"},
      "timeoutSecs": 30,
      "retries": 1
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

For API-only development, start PostgreSQL and Redis, then run the Spring Boot app. Kafka admin topic creation is disabled by default so the service can boot without a broker unless scheduler/worker features are enabled.

```bash
docker compose up -d postgres redis
```

```bash
mvn spring-boot:run
```

For the distributed demo stack with leader election, Kafka dispatch, DLQ, gRPC worker registration, and Prometheus:

```bash
docker compose up --build
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

Inspect a run timeline:

```bash
curl http://localhost:8080/api/runs/{dagRunId}/timeline
```

Pause or resume scheduling:

```bash
curl -X POST http://localhost:8080/api/admin/scheduler/pause
curl -X POST http://localhost:8080/api/admin/scheduler/resume
curl -X POST http://localhost:8080/api/admin/scheduler/dags/daily-pipeline/pause
curl -X POST http://localhost:8080/api/admin/scheduler/dags/daily-pipeline/resume
```

Inspect and manually requeue DLQ tasks:

```bash
curl http://localhost:8080/api/admin/dlq
curl -X POST http://localhost:8080/api/admin/dlq/{dlqTaskId}/requeue
```

Prometheus metrics are exposed at:

```bash
curl http://localhost:8080/actuator/prometheus
```

Generate a 1,000-task load-test DAG:

```bash
./scripts/generate-load-dag.sh > /tmp/flowmesh-1000-task-dag.json
curl -X POST http://localhost:8080/api/dags \
  -H 'Content-Type: application/json' \
  -d @/tmp/flowmesh-1000-task-dag.json
```

After creating a run, scheduling latency is exposed as the `task_scheduling_latency_ms_milliseconds` histogram on `/actuator/prometheus`; use `histogram_quantile(0.99, rate(task_scheduling_latency_ms_milliseconds_bucket[5m]))` in Prometheus for P99.

Latest local Compose smoke test, recorded 2026-05-26: a 1,000-task root-plus-fanout DAG completed with all 1,000 task runs in `SUCCESS`; worker completion logs were distributed across all five workers (`201/183/222/201/193`), and the scheduler histogram estimated P99 scheduling latency at about `25,974 ms` with the default `batch-size=50` and `poll-delay-ms=1000`.

Run tests:

```bash
mvn test
```

## Deploy scaffold

`deploy/ecs/flowmesh-ecs.yml` defines ECS/Fargate services for three scheduler tasks and five worker tasks. Provide an image URI plus managed PostgreSQL, Redis, and Kafka endpoints when launching the stack.
