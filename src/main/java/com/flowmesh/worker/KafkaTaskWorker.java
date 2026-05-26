package com.flowmesh.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowmesh.common.logging.MdcScopes;
import com.flowmesh.dag.run.TaskLifecycleService;
import com.flowmesh.kafka.TaskMessage;
import com.flowmesh.worker.handler.TaskContext;
import com.flowmesh.worker.handler.TaskHandlerResult;
import com.flowmesh.worker.handler.TaskHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

@Component
@ConditionalOnProperty(prefix = "flowmesh.worker", name = "enabled", havingValue = "true")
public class KafkaTaskWorker {
    private static final Logger log = LoggerFactory.getLogger(KafkaTaskWorker.class);

    private final WorkerProperties properties;
    private final WorkerRuntimeState runtimeState;
    private final TaskLifecycleService lifecycleService;
    private final TaskHandlerRegistry handlerRegistry;
    private final ObjectMapper objectMapper;
    private final Set<String> supportedTypes;

    public KafkaTaskWorker(
            WorkerProperties properties,
            WorkerRuntimeState runtimeState,
            TaskLifecycleService lifecycleService,
            TaskHandlerRegistry handlerRegistry,
            ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.runtimeState = runtimeState;
        this.lifecycleService = lifecycleService;
        this.handlerRegistry = handlerRegistry;
        this.objectMapper = objectMapper;
        this.supportedTypes = new HashSet<>(properties.supportedTaskTypes());
    }

    @KafkaListener(
            topics = "#{@workerTaskTopics}",
            groupId = "${flowmesh.worker.group-id:flowmesh-workers}",
            concurrency = "${flowmesh.worker.max-concurrent:4}"
    )
    public void consume(TaskMessage message) throws Exception {
        if (!supportedTypes.contains(message.type())) {
            log.debug("Skipping unsupported task type {}", message.type());
            return;
        }

        try (MdcScopes.Scope ignored = MdcScopes.task(message.dagRunId(), message.taskId(), properties.workerId())) {
            boolean shouldExecute = lifecycleService.startTask(message, properties.workerId());
            if (!shouldExecute) {
                log.info("Skipping duplicate or already-running task idempotencyKey={}", message.idempotencyKey());
                return;
            }

            Instant startedAt = Instant.now();
            runtimeState.started(message);
            try {
                JsonNode config = objectMapper.readTree(message.configJson());
                TaskHandlerResult result = handlerRegistry.handlerFor(message.type())
                        .execute(new TaskContext(message, config));
                if (result.success()) {
                    lifecycleService.completeSuccess(message, properties.workerId(), startedAt);
                    log.info("Task completed successfully");
                } else {
                    lifecycleService.completeFailure(message, properties.workerId(), result.message());
                    log.warn("Task failed: {}", result.message());
                }
            } catch (Exception exception) {
                lifecycleService.completeFailure(message, properties.workerId(), exception.getMessage());
                log.warn("Task execution threw exception", exception);
            } finally {
                runtimeState.finished(message);
            }
        }
    }
}
