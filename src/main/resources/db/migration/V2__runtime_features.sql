alter table task_runs
    add column config_json jsonb not null default '{}'::jsonb,
    add column success_branch_task_id varchar(160),
    add column failure_branch_task_id varchar(160),
    add column next_attempt_at timestamptz,
    add column last_heartbeat_at timestamptz,
    add column worker_id varchar(160),
    add column error_message text;

create index idx_task_runs_retry_time on task_runs (state, next_attempt_at);
create index idx_task_runs_running_heartbeat on task_runs (state, last_heartbeat_at, started_at);

create table task_state_transitions (
    id uuid primary key,
    dag_run_id uuid not null references dag_runs (id) on delete cascade,
    task_run_id uuid not null references task_runs (id) on delete cascade,
    task_id varchar(160) not null,
    from_state varchar(24),
    to_state varchar(24) not null,
    reason varchar(240),
    details_json jsonb not null default '{}'::jsonb,
    transitioned_at timestamptz not null
);

create index idx_task_state_transitions_run_time on task_state_transitions (dag_run_id, transitioned_at);
create index idx_task_state_transitions_task_time on task_state_transitions (task_run_id, transitioned_at);

create table task_deduplication (
    idempotency_key varchar(320) primary key,
    dag_run_id uuid not null,
    task_id varchar(160) not null,
    worker_id varchar(160),
    created_at timestamptz not null
);

create table dlq_tasks (
    id uuid primary key,
    dag_run_id uuid not null,
    task_id varchar(160) not null,
    task_type varchar(120) not null,
    idempotency_key varchar(320) not null,
    payload_json jsonb not null,
    error_message text,
    created_at timestamptz not null,
    requeued_at timestamptz
);

create index idx_dlq_tasks_created_at on dlq_tasks (created_at);

create table worker_registrations (
    worker_id varchar(160) primary key,
    supported_task_types jsonb not null,
    max_concurrent integer not null,
    current_task_id varchar(160),
    status varchar(32) not null,
    last_heartbeat_at timestamptz,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create index idx_worker_registrations_status on worker_registrations (status, last_heartbeat_at);
