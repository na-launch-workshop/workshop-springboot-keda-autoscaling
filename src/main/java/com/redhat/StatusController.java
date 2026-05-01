package com.redhat;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RestController
public class StatusController {

    @Value("${spring.kafka.bootstrap-servers:kafka-kafka-bootstrap:9092}")
    private String bootstrapServers;

    @Value("${kafka.topic:keda-events}")
    private String topic;

    @Value("${kafka.consumer.group:keda-consumer-group}")
    private String consumerGroup;

    private AdminClient adminClient;

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        adminClient = AdminClient.create(props);
    }

    @PreDestroy
    public void destroy() {
        if (adminClient != null) adminClient.close();
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> result = new HashMap<>();
        result.put("topic", topic);
        result.put("consumerGroup", consumerGroup);

        try {
            Map<TopicPartition, OffsetAndMetadata> committed = adminClient
                    .listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata()
                    .get();

            Map<TopicPartition, OffsetSpec> endOffsetRequest = new HashMap<>();
            for (int i = 0; i < 5; i++) {
                endOffsetRequest.put(new TopicPartition(topic, i), OffsetSpec.latest());
            }

            ListOffsetsResult endOffsetsResult = adminClient.listOffsets(endOffsetRequest);
            long totalLag = 0;
            Map<Integer, Long> lagPerPartition = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetSpec> entry : endOffsetRequest.entrySet()) {
                TopicPartition tp = entry.getKey();
                long endOffset = endOffsetsResult.partitionResult(tp).get().offset();
                long committedOffset = committed.containsKey(tp) ? committed.get(tp).offset() : 0;
                long lag = Math.max(0, endOffset - committedOffset);
                lagPerPartition.put(tp.partition(), lag);
                totalLag += lag;
            }

            result.put("totalLag", totalLag);
            result.put("lagPerPartition", lagPerPartition);
        } catch (Exception e) {
            result.put("totalLag", -1);
            result.put("error", e.getMessage());
        }

        return result;
    }
}
