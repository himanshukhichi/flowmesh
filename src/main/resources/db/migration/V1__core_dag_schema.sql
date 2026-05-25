create table dag_definitions (
    id uuid primary key,
    dag_id varchar(120) not null,
    version integer not null,
    name varchar(240) not null,
    definition_json jsonb not null,
    execution_order_json jsonb not null,
    created_at timestamptz not null,
    constraint uk_dag_definitions_dag_id_version unique (dag_id, version)
);

create index idx_dag_definitions_dag_id on dag_definitions (dag_id);

create table dag_runs (
    id uuid primary key,
    dag_definition_id uuid not null references dag_definitions (id),
    dag_id varchar(120) not null,
    version integer not null,
    status varchar(24) not null,
    created_at timestamptz not null,
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null
);

create index idx_dag_runs_dag_id_version on dag_runs (dag_id, version);

create table task_runs (
    id uuid primary key,
    dag_run_id uuid not null references dag_runs (id) on delete cascade,
    task_id varchar(160) not null,
    type varchar(120) not null,
    state varchar(24) not null,
    attempt integer not null,
    timeout_secs integer not null,
    retries integer not null,
    created_at timestamptz not null,
    queued_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz not null,
    constraint uk_task_runs_dag_run_task unique (dag_run_id, task_id)
);

create index idx_task_runs_state_created_at on task_runs (state, created_at);
create index idx_task_runs_dag_run_state on task_runs (dag_run_id, state);

create table task_run_dependencies (
    task_run_id uuid not null references task_runs (id) on delete cascade,
    depends_on_task_id varchar(160) not null,
    primary key (task_run_id, depends_on_task_id)
);
