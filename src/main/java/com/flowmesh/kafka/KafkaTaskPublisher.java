package com.flowmesh.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowmesh.kafka.outbox.TaskDispatchOutboxEntity;
import com.flowmesh.kafka.outbox.TaskDispatchOutboxRepository;
import org.springframework.stereotype.Component;

@Component
public class KafkaTaskPublisher implements TaskPublisher {
    private static final String TASK_PAYLOAD = "TASK";
    private static final String RETRY_PAYLOAD = "RETRY";
    private static final String DLQ_PAYLOAD = "DLQ";

    private final TaskDispatchOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public KafkaTaskPublisher(TaskDispatchOutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(TaskMessage message) {
        enqueue(message.topic(), message.idempotencyKey(), TASK_PAYLOAD, message);
    }

    @Override
    public void publishRetry(RetryTaskMessage message) {
        enqueue("tasks.retry", message.idempotencyKey(), RETRY_PAYLOAD, message);
    }

    @Override
    public void publishDlq(DlqTaskMessage message) {
        enqueue("tasks.dlq", message.idempotencyKey(), DLQ_PAYLOAD, message);
    }

    private void enqueue(String topic, String key, String payloadType, Object message) {
        outboxRepository.save(TaskDispatchOutboxEntity.create(topic, key, payloadType, writeJson(message)));
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize Kafka outbox payload", exception);
        }
    }
}
