package com.redhat.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class EventProducer {

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 100;
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-kafka-bootstrap:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "keda-events");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.printf("Sending %d messages to topic '%s'...%n", count, topic);
            for (int i = 1; i <= count; i++) {
                String value = String.format("{\"id\":%d,\"message\":\"work item %d of %d\"}", i, i, count);
                producer.send(new ProducerRecord<>(topic, UUID.randomUUID().toString(), value));
                System.out.printf("\r  Sent %d / %d", i, count);
            }
            producer.flush();
            System.out.printf("%n%d messages sent. Watch the lag build in Kafka UI.%n", count);
        }
    }
}
