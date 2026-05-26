package com.flowmesh.dlq;

import com.flowmesh.kafka.DlqTaskMessage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "flowmesh.dlq", name = "consumer-enabled", havingValue = "true")
public class DlqConsumer {
    private final DlqService dlqService;

    public DlqConsumer(DlqService dlqService) {
        this.dlqService = dlqService;
    }

    @KafkaListener(topics = "tasks.dlq", groupId = "${flowmesh.dlq.group-id:flowmesh-dlq}")
    public void consume(DlqTaskMessage message) {
        dlqService.record(message);
    }
}
