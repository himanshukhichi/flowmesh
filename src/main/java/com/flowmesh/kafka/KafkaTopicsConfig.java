package com.flowmesh.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {
    private static final int DEFAULT_PARTITIONS = 20;
    private static final short DEFAULT_REPLICAS = 1;

    @Bean
    NewTopic httpCallTasksTopic() {
        return taskTopic("tasks.http_call");
    }

    @Bean
    NewTopic sqlQueryTasksTopic() {
        return taskTopic("tasks.sql_query");
    }

    @Bean
    NewTopic mlInferenceTasksTopic() {
        return taskTopic("tasks.ml_inference");
    }

    @Bean
    NewTopic retryTopic() {
        return taskTopic("tasks.retry");
    }

    @Bean
    NewTopic dlqTopic() {
        return taskTopic("tasks.dlq");
    }

    private NewTopic taskTopic(String name) {
        return TopicBuilder.name(name)
                .partitions(DEFAULT_PARTITIONS)
                .replicas(DEFAULT_REPLICAS)
                .build();
    }
}
