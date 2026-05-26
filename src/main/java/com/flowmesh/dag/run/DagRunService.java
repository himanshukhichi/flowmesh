package com.flowmesh.dag.run;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowmesh.dag.api.DagRunResponse;
import com.flowmesh.dag.model.DagDefinition;
import com.flowmesh.dag.model.ValidatedDag;
import com.flowmesh.dag.persistence.DagDefinitionEntity;
import com.flowmesh.dag.persistence.DagDefinitionRepository;
import com.flowmesh.dag.service.DagValidationException;
import com.flowmesh.dag.service.DagValidationService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashSet;
import java.util.Set;

@Service
public class DagRunService {
    private final DagDefinitionRepository dagDefinitionRepository;
    private final DagRunRepository dagRunRepository;
    private final TaskRunRepository taskRunRepository;
    private final TaskStateMachineService taskStateMachineService;
    private final DagValidationService dagValidationService;
    private final ObjectMapper objectMapper;

    public DagRunService(
            DagDefinitionRepository dagDefinitionRepository,
            DagRunRepository dagRunRepository,
            TaskRunRepository taskRunRepository,
            TaskStateMachineService taskStateMachineService,
            DagValidationService dagValidationService,
            ObjectMapper objectMapper
    ) {
        this.dagDefinitionRepository = dagDefinitionRepository;
        this.dagRunRepository = dagRunRepository;
        this.taskRunRepository = taskRunRepository;
        this.taskStateMachineService = taskStateMachineService;
        this.dagValidationService = dagValidationService;
        this.objectMapper = objectMapper;
    }

    @Transactional(transactionManager = "transactionManager")
    public DagRunResponse createRun(String dagId, Integer requestedVersion) {
        DagDefinitionEntity definitionEntity = findDefinition(dagId, requestedVersion);
        DagDefinition definition = readDefinition(definitionEntity);
        ValidatedDag validatedDag = dagValidationService.validate(definition);

        DagRunEntity newRun = DagRunEntity.create(definitionEntity);
        newRun.markRunning();
        DagRunEntity run = dagRunRepository.save(newRun);

        Set<String> initiallyReady = new LinkedHashSet<>(validatedDag.initialReadyTaskIds());
        definition.tasks().forEach(task -> {
            TaskRunEntity taskRun = taskRunRepository.save(TaskRunEntity.create(
                    run,
                    task,
                    TaskState.CREATED,
                    writeJson(task.config())
            ));
            taskStateMachineService.transition(taskRun, TaskState.PENDING, "dag_run_created");
        });

        return new DagRunResponse(
                run.getId(),
                run.getDagId(),
                run.getVersion(),
                run.getStatus(),
                validatedDag.initialReadyTaskIds()
        );
    }

    private DagDefinitionEntity findDefinition(String dagId, Integer requestedVersion) {
        if (requestedVersion == null) {
            return dagDefinitionRepository.findTopByDagIdOrderByVersionDesc(dagId)
                    .orElseThrow(() -> new DagValidationException("DAG_NOT_FOUND", "DAG '" + dagId + "' was not found"));
        }

        return dagDefinitionRepository.findByDagIdAndVersion(dagId, requestedVersion)
                .orElseThrow(() -> new DagValidationException(
                        "DAG_VERSION_NOT_FOUND",
                        "DAG '" + dagId + "' version " + requestedVersion + " was not found"
                ));
    }

    private DagDefinition readDefinition(DagDefinitionEntity definitionEntity) {
        try {
            return objectMapper.readValue(definitionEntity.getDefinitionJson(), DagDefinition.class);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Persisted DAG definition is not readable", exception);
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize task config", exception);
        }
    }
}
