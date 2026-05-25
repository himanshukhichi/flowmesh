package com.flowmesh.dag.api;

import com.flowmesh.dag.model.DagDefinition;
import com.flowmesh.dag.run.DagRunService;
import com.flowmesh.dag.service.DagDefinitionParser;
import com.flowmesh.dag.service.DagSubmissionService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Valid;
import jakarta.validation.Validator;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.flowmesh.dag.service.DagQueryService;

import java.util.Set;

@RestController
@RequestMapping("/api")
public class DagController {
    private static final String APPLICATION_YAML = "application/yaml";
    private static final String APPLICATION_X_YAML = "application/x-yaml";
    private static final String TEXT_YAML = "text/yaml";

    private final DagSubmissionService dagSubmissionService;
    private final DagDefinitionParser dagDefinitionParser;
    private final DagQueryService dagQueryService;
    private final DagRunService dagRunService;
    private final Validator validator;

    public DagController(
            DagSubmissionService dagSubmissionService,
            DagDefinitionParser dagDefinitionParser,
            DagQueryService dagQueryService,
            DagRunService dagRunService,
            Validator validator
    ) {
        this.dagSubmissionService = dagSubmissionService;
        this.dagDefinitionParser = dagDefinitionParser;
        this.dagQueryService = dagQueryService;
        this.dagRunService = dagRunService;
        this.validator = validator;
    }

    @PostMapping(path = "/dags", consumes = MediaType.APPLICATION_JSON_VALUE)
    public DagSubmissionResponse submitJson(@Valid @RequestBody DagDefinition definition) {
        return dagSubmissionService.submit(definition);
    }

    @PostMapping(path = "/dags", consumes = {APPLICATION_YAML, APPLICATION_X_YAML, TEXT_YAML})
    public DagSubmissionResponse submitYaml(@RequestBody String yaml) {
        DagDefinition definition = dagDefinitionParser.parseYaml(yaml);
        validate(definition);
        return dagSubmissionService.submit(definition);
    }

    @GetMapping("/dags/{dagId}/versions/latest")
    public DagVersionResponse getLatestVersion(@PathVariable String dagId) {
        return dagQueryService.getLatestVersion(dagId);
    }

    @PostMapping("/dags/{dagId}/runs")
    public DagRunResponse createRun(
            @PathVariable String dagId,
            @RequestParam(name = "version", required = false) Integer version
    ) {
        return dagRunService.createRun(dagId, version);
    }

    private void validate(DagDefinition definition) {
        Set<ConstraintViolation<DagDefinition>> violations = validator.validate(definition);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }
    }
}
