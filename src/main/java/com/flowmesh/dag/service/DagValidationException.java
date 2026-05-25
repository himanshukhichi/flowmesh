package com.flowmesh.dag.service;

public class DagValidationException extends RuntimeException {
    private final String error;

    public DagValidationException(String error, String message) {
        super(message);
        this.error = error;
    }

    public String error() {
        return error;
    }
}
