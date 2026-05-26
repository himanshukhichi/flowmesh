package com.flowmesh.worker.grpc;

import com.flowmesh.grpc.WorkerHeartbeatRequest;
import com.flowmesh.grpc.WorkerRegistrationRequest;
import com.flowmesh.grpc.WorkerRegistryGrpc;
import com.flowmesh.kafka.TaskMessage;
import com.flowmesh.worker.WorkerProperties;
import com.flowmesh.worker.WorkerRuntimeState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(prefix = "flowmesh.worker", name = "enabled", havingValue = "true")
public class WorkerRegistrationClient {
    private static final Logger log = LoggerFactory.getLogger(WorkerRegistrationClient.class);

    private final WorkerProperties properties;
    private final WorkerRuntimeState runtimeState;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private ManagedChannel channel;
    private WorkerRegistryGrpc.WorkerRegistryBlockingStub stub;

    public WorkerRegistrationClient(WorkerProperties properties, WorkerRuntimeState runtimeState) {
        this.properties = properties;
        this.runtimeState = runtimeState;
    }

    @PostConstruct
    void start() {
        this.channel = ManagedChannelBuilder
                .forAddress(properties.schedulerGrpcHost(), properties.schedulerGrpcPort())
                .usePlaintext()
                .build();
        this.stub = WorkerRegistryGrpc.newBlockingStub(channel);
    }

    @Scheduled(
            initialDelayString = "${flowmesh.worker.registration-initial-delay-ms:2000}",
            fixedDelayString = "${flowmesh.worker.registration-retry-ms:5000}"
    )
    public void ensureRegistered() {
        if (registered.get()) {
            return;
        }
        try {
            register();
        } catch (StatusRuntimeException exception) {
            log.warn(
                    "Worker {} registration failed; retrying later: {}",
                    properties.workerId(),
                    exception.getStatus()
            );
        }
    }

    @Scheduled(fixedDelayString = "${flowmesh.worker.heartbeat-ms:10000}")
    public void heartbeat() {
        if (!registered.get()) {
            ensureRegistered();
        }
        if (!registered.get()) {
            return;
        }

        var currentTasks = runtimeState.currentTasks();
        if (currentTasks.isEmpty()) {
            sendHeartbeat(null, "IDLE");
            return;
        }

        for (TaskMessage currentTask : currentTasks) {
            sendHeartbeat(currentTask, "RUNNING");
        }
    }

    @PreDestroy
    void stop() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    private void register() {
        stub.registerWorker(WorkerRegistrationRequest.newBuilder()
                .setWorkerId(properties.workerId())
                .addAllSupportedTaskTypes(properties.supportedTaskTypes())
                .setMaxConcurrent(properties.maxConcurrent())
                .build());
        registered.set(true);
        log.info("Worker {} registered with scheduler", properties.workerId());
    }

    private void sendHeartbeat(TaskMessage currentTask, String status) {
        WorkerHeartbeatRequest.Builder request = WorkerHeartbeatRequest.newBuilder()
                .setWorkerId(properties.workerId())
                .setStatus(status)
                .setObservedAtEpochMillis(Instant.now().toEpochMilli());
        if (currentTask != null) {
            request.setDagRunId(currentTask.dagRunId().toString());
            request.setTaskId(currentTask.taskId());
        }
        try {
            stub.heartbeat(request.build());
        } catch (StatusRuntimeException exception) {
            registered.set(false);
            log.warn(
                    "Worker {} heartbeat failed; registration will be retried: {}",
                    properties.workerId(),
                    exception.getStatus()
            );
        }
    }
}
