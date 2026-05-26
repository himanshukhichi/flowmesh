package com.flowmesh.worker;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkerKafkaTopicsConfig {

    @Bean("workerTaskTopics")
    String[] workerTaskTopics(WorkerProperties properties) {
        return properties.supportedTaskTypes().stream()
                .map(type -> "tasks." + type)
                .toArray(String[]::new);
    }
}
