package com.flowmesh.common.logging;

import org.slf4j.MDC;

import java.util.UUID;

public final class MdcScopes {
    private MdcScopes() {
    }

    public static Scope task(UUID dagRunId, String taskId, String workerId) {
        put("dagRunId", dagRunId == null ? null : dagRunId.toString());
        put("taskId", taskId);
        put("workerId", workerId);
        return MDC::clear;
    }

    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    private static void put(String key, String value) {
        if (value == null || value.isBlank()) {
            MDC.remove(key);
        } else {
            MDC.put(key, value);
        }
    }
}
