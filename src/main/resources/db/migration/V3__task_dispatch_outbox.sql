create table task_dispatch_outbox (
    id uuid primary key,
    topic varchar(240) not null,
    message_key varchar(320) not null,
    payload_type varchar(32) not null,
    payload_json jsonb not null,
    state varchar(24) not null,
    publish_attempts integer not null,
    next_attempt_at timestamptz,
    created_at timestamptz not null,
    published_at timestamptz,
    updated_at timestamptz not null,
    error_message text
);

create index idx_task_dispatch_outbox_pending on task_dispatch_outbox (state, next_attempt_at, created_at);
