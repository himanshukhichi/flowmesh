package com.flowmesh.worker.grpc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class WorkerRegistryService {
    private final WorkerRegistrationRepository repository;
    private final ObjectMapper objectMapper;

    public WorkerRegistryService(WorkerRegistrationRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Transactional(transactionManager = "transactionManager")
    public void register(String workerId, List<String> supportedTaskTypes, int maxConcurrent) {
        String supportedTypesJson = writeJson(supportedTaskTypes);
        WorkerRegistrationEntity entity = repository.findById(workerId)
                .orElseGet(() -> WorkerRegistrationEntity.create(workerId, supportedTypesJson, maxConcurrent));
        entity.updateRegistration(supportedTypesJson, maxConcurrent);
        repository.save(entity);
    }

    @Transactional(transactionManager = "transactionManager")
    public void heartbeat(String workerId, String currentTaskId) {
        repository.findById(workerId).ifPresent(entity -> entity.heartbeat(currentTaskId));
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize supported task types", exception);
        }
    }
}
