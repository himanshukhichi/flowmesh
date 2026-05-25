package com.flowmesh.dag.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.flowmesh.dag.model.DagDefinition;
import org.springframework.stereotype.Service;

@Service
public class DagDefinitionParser {
    private final ObjectMapper yamlMapper;

    public DagDefinitionParser(ObjectMapper objectMapper) {
        this.yamlMapper = objectMapper.copyWith(new YAMLFactory())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public DagDefinition parseYaml(String yaml) {
        try {
            return yamlMapper.readValue(yaml, DagDefinition.class);
        } catch (JsonProcessingException exception) {
            throw new DagValidationException("INVALID_DAG", "Unable to parse DAG YAML");
        }
    }
}
