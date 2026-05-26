package com.flowmesh.worker.grpc;

import com.flowmesh.dag.run.TaskLifecycleService;
import com.flowmesh.grpc.WorkerHeartbeatRequest;
import com.flowmesh.grpc.WorkerHeartbeatResponse;
import com.flowmesh.grpc.WorkerRegistrationRequest;
import com.flowmesh.grpc.WorkerRegistrationResponse;
import com.flowmesh.grpc.WorkerRegistryGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class WorkerRegistryGrpcService extends WorkerRegistryGrpc.WorkerRegistryImplBase {
    private final WorkerRegistryService workerRegistryService;
    private final TaskLifecycleService taskLifecycleService;

    public WorkerRegistryGrpcService(
            WorkerRegistryService workerRegistryService,
            TaskLifecycleService taskLifecycleService
    ) {
        this.workerRegistryService = workerRegistryService;
        this.taskLifecycleService = taskLifecycleService;
    }

    @Override
    public void registerWorker(
            WorkerRegistrationRequest request,
            StreamObserver<WorkerRegistrationResponse> responseObserver
    ) {
        workerRegistryService.register(
                request.getWorkerId(),
                request.getSupportedTaskTypesList(),
                request.getMaxConcurrent()
        );
        responseObserver.onNext(WorkerRegistrationResponse.newBuilder()
                .setAccepted(true)
                .setMessage("registered")
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(
            WorkerHeartbeatRequest request,
            StreamObserver<WorkerHeartbeatResponse> responseObserver
    ) {
        String taskId = request.getTaskId().isBlank() ? null : request.getTaskId();
        workerRegistryService.heartbeat(request.getWorkerId(), taskId);
        if (taskId != null && !request.getDagRunId().isBlank()) {
            taskLifecycleService.recordHeartbeat(request.getDagRunId(), taskId, request.getWorkerId());
        }
        responseObserver.onNext(WorkerHeartbeatResponse.newBuilder().setAccepted(true).build());
        responseObserver.onCompleted();
    }
}
