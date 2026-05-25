package com.flowmesh.dag.run;

public enum TaskState {
    CREATED,
    PENDING,
    QUEUED,
    RUNNING,
    SUCCESS,
    FAILED,
    RETRYING,
    DLQ
}
