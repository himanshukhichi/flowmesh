package com.flowmesh.dag.service;

import java.util.List;

public class CycleDetectedException extends DagValidationException {
    private final List<String> path;

    public CycleDetectedException(List<String> path) {
        super("CYCLE_DETECTED", "DAG contains a cycle: " + String.join(" -> ", path));
        this.path = List.copyOf(path);
    }

    public List<String> path() {
        return path;
    }
}
