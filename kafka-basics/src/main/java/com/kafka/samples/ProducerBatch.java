package com.kafka.samples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerBatch {
    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class.getSimpleName());

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

        for (int i=0; i<5; i++) {
            // Create a producer record (message)
            String kafkaMessage = "Message from java : " + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, kafkaMessage);

            // Send the record
            producer.send(producerRecord);


            try{
                Thread.sleep(5000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }

            producer.flush();

        }
        // Flush and close the producer
        // send the records immediately and completed successfuly when ack is received


        // wait for all incomplete messages to be sent and close the producer
        producer.close();
    }
}
