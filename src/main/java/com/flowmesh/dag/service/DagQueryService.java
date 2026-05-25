package com.flowmesh.dag.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowmesh.dag.api.DagVersionResponse;
import com.flowmesh.dag.model.DagDefinition;
import com.flowmesh.dag.persistence.DagDefinitionEntity;
import com.flowmesh.dag.persistence.DagDefinitionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class DagQueryService {
    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() {
    };

    private final DagDefinitionRepository dagDefinitionRepository;
    private final ObjectMapper objectMapper;

    public DagQueryService(DagDefinitionRepository dagDefinitionRepository, ObjectMapper objectMapper) {
        this.dagDefinitionRepository = dagDefinitionRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional(readOnly = true)
    public DagVersionResponse getLatestVersion(String dagId) {
        DagDefinitionEntity entity = dagDefinitionRepository.findTopByDagIdOrderByVersionDesc(dagId)
                .orElseThrow(() -> new DagValidationException("DAG_NOT_FOUND", "DAG '" + dagId + "' was not found"));
        return toResponse(entity);
    }

    private DagVersionResponse toResponse(DagDefinitionEntity entity) {
        DagDefinition definition = readValue(entity.getDefinitionJson(), DagDefinition.class);
        List<String> executionOrder = readValue(entity.getExecutionOrderJson(), STRING_LIST);

        return new DagVersionResponse(
                entity.getId(),
                entity.getDagId(),
                entity.getVersion(),
                entity.getName(),
                definition.tasks().size(),
                executionOrder,
                entity.getCreatedAt()
        );
    }

    private <T> T readValue(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Persisted DAG data is not readable", exception);
        }
    }

    private <T> T readValue(String json, TypeReference<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Persisted DAG data is not readable", exception);
        }
    }
}
