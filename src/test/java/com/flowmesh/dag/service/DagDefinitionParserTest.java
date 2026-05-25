package com.flowmesh.dag.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowmesh.dag.model.DagDefinition;
import com.flowmesh.dag.model.TaskDefinition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DagDefinitionParserTest {

    @Test
    void parsesYamlDagDefinitionsAndAppliesTaskDefaults() {
        DagDefinitionParser parser = new DagDefinitionParser(new ObjectMapper());

        DagDefinition definition = parser.parseYaml("""
                dagId: hello-flow
                name: Hello Flow
                tasks:
                  - taskId: fetch
                    type: http_call
                    config:
                      url: https://example.test
                  - taskId: load
                    type: sql_query
                    dependsOn: [fetch]
                """);

        TaskDefinition fetch = definition.tasks().getFirst();
        TaskDefinition load = definition.tasks().get(1);

        assertThat(definition.dagId()).isEqualTo("hello-flow");
        assertThat(fetch.dependsOn()).isEmpty();
        assertThat(fetch.timeoutSecs()).isEqualTo(TaskDefinition.DEFAULT_TIMEOUT_SECS);
        assertThat(fetch.retries()).isEqualTo(TaskDefinition.DEFAULT_RETRIES);
        assertThat(load.dependsOn()).containsExactly("fetch");
    }
}
