package com.flowmesh.kafka.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowmesh.kafka.DlqTaskMessage;
import com.flowmesh.kafka.RetryTaskMessage;
import com.flowmesh.kafka.TaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaOutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaOutboxPublisher.class);
    private static final int SEND_TIMEOUT_SECS = 10;

    private final TaskDispatchOutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    public KafkaOutboxPublisher(
            TaskDispatchOutboxRepository outboxRepository,
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${flowmesh.outbox.batch-size:50}") int batchSize
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${flowmesh.outbox.publish-delay-ms:500}")
    @Transactional(transactionManager = "transactionManager")
    public void publishPending() {
        for (TaskDispatchOutboxEntity message : outboxRepository.lockPending(batchSize)) {
            try {
                sendTransactionally(message.getTopic(), message.getMessageKey(), readPayload(message));
                message.markSent();
            } catch (Exception exception) {
                message.markPublishFailed(exception.getMessage());
                log.warn("Kafka outbox publish failed topic={} key={}", message.getTopic(), message.getMessageKey(), exception);
            }
        }
    }

    private Object readPayload(TaskDispatchOutboxEntity message) throws JsonProcessingException {
        return switch (message.getPayloadType()) {
            case "TASK" -> objectMapper.readValue(message.getPayloadJson(), TaskMessage.class);
            case "RETRY" -> objectMapper.readValue(message.getPayloadJson(), RetryTaskMessage.class);
            case "DLQ" -> objectMapper.readValue(message.getPayloadJson(), DlqTaskMessage.class);
            default -> throw new IllegalArgumentException("Unknown outbox payload type " + message.getPayloadType());
        };
    }

    private void sendTransactionally(String topic, String key, Object message) {
        kafkaTemplate.executeInTransaction(operations -> {
            try {
                operations.send(topic, key, message).get(SEND_TIMEOUT_SECS, TimeUnit.SECONDS);
                return null;
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while publishing Kafka message to " + topic, exception);
            } catch (ExecutionException | TimeoutException exception) {
                throw new IllegalStateException("Unable to publish Kafka message to " + topic, exception);
            }
        });
    }
}
