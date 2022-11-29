package com.kafka.samples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerCallbackWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting Kafka Consumer Client");
        String bootstrapServer  ="127.0.0.1:9092";
        String topic = "java_firmalar";
        String groupId = "orjjavagrp";
        // Create consumer properties
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer with properties
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // Subscribe to a topic or topics
        consumer.subscribe(Collections.singleton(topic));

        // Fetch messages and process them (.poll() method) until cancel in a loop
        log.info("Start polling...");
        while (true) {
            log.info("Fetch messages from last committed offset...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // Iterate through records and process !!
            for (ConsumerRecord<String, String> record: records){
                log.info("Consumer Record : " + record.key() + ", " + record.value() );
            }
        }

        // Close the consumer

    }
}
