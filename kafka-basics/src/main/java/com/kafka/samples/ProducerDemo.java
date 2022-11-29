package com.kafka.samples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting Kafka Producer Client");
        String bootstrapServer  ="127.0.0.1:9092";
        String topic = "java_firmalar";

        // Create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer with properties
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // Create a producer record (message)
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "message from java client");

        // Send the record
        producer.send(producerRecord);

        // Flush and close the producer
        // send the records immediately and completed successfuly when ack is received
        producer.flush();

        // wait for all incomplete messages to be sent and close the producer
        producer.close();
    }
}
