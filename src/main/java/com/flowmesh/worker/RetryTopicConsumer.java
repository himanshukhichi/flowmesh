package com.flowmesh.worker;

import com.flowmesh.kafka.RetryTaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "flowmesh.retry", name = "consumer-enabled", havingValue = "true")
public class RetryTopicConsumer {
    private static final Logger log = LoggerFactory.getLogger(RetryTopicConsumer.class);

    @KafkaListener(topics = "tasks.retry", groupId = "${flowmesh.retry.group-id:flowmesh-retry}")
    public void consume(RetryTaskMessage message) {
        log.info(
                "Retry scheduled dagRunId={} taskId={} nextAttempt={} notBeforeEpochMillis={}",
                message.dagRunId(),
                message.taskId(),
                message.nextAttempt(),
                message.notBeforeEpochMillis()
        );
    }
}
