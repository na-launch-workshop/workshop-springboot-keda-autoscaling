package com.redhat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;

@Component
public class KafkaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);

    private final long delayMs;

    public KafkaEventConsumer(@Value("${demo.processing.delay-ms:2500}") long delayMs) {
        this.delayMs = delayMs;
    }

    @KafkaListener(topics = "${kafka.topic:keda-events}", groupId = "${kafka.consumer.group:keda-consumer-group}")
    public void handle(String message) throws InterruptedException {
        log.info("Processing: {}", message);
        Thread.sleep(delayMs);
        log.info("Completed at {}: {}", OffsetDateTime.now(), message);
    }
}
