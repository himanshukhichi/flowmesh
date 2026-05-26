package com.flowmesh.dag.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowmesh.dag.api.DagSubmissionResponse;
import com.flowmesh.dag.model.DagDefinition;
import com.flowmesh.dag.model.ValidatedDag;
import com.flowmesh.dag.persistence.DagDefinitionEntity;
import com.flowmesh.dag.persistence.DagDefinitionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DagSubmissionService {
    private final DagDefinitionRepository dagDefinitionRepository;
    private final DagValidationService dagValidationService;
    private final ObjectMapper objectMapper;

    public DagSubmissionService(
            DagDefinitionRepository dagDefinitionRepository,
            DagValidationService dagValidationService,
            ObjectMapper objectMapper
    ) {
        this.dagDefinitionRepository = dagDefinitionRepository;
        this.dagValidationService = dagValidationService;
        this.objectMapper = objectMapper;
    }

    @Transactional(transactionManager = "transactionManager")
    public DagSubmissionResponse submit(DagDefinition definition) {
        ValidatedDag validatedDag = dagValidationService.validate(definition);
        int nextVersion = dagDefinitionRepository.findTopByDagIdOrderByVersionDesc(definition.dagId())
                .map(existing -> existing.getVersion() + 1)
                .orElse(1);

        DagDefinitionEntity entity = DagDefinitionEntity.create(
                definition.dagId(),
                nextVersion,
                definition.name(),
                writeJson(definition),
                writeJson(validatedDag.executionOrder())
        );
        DagDefinitionEntity saved = dagDefinitionRepository.save(entity);

        return new DagSubmissionResponse(
                saved.getDagId(),
                saved.getVersion(),
                saved.getName(),
                definition.tasks().size(),
                validatedDag.executionOrder(),
                saved.getCreatedAt()
        );
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize DAG definition", exception);
        }
    }
}
