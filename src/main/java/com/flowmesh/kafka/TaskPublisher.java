package com.flowmesh.kafka;

public interface TaskPublisher {
    void publish(TaskMessage message);

    void publishRetry(RetryTaskMessage message);

    void publishDlq(DlqTaskMessage message);
}
