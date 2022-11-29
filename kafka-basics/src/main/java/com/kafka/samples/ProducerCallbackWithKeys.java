package com.kafka.samples;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallbackWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerCallbackWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting Kafka Producer Client");
        String bootstrapServer  ="127.0.0.1:9092";
        String topic = "java_firmalar";

        // Create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Avoid kafka producer from doing batches (default value is 16k)
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(10));

        // Create producer with properties
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        for (int i=0; i<30; i++) {
            // Create a producer record (message)
            String kafkaKey = "javamsg_" + i;
            String kafkaMessage = "Message with key : " + i;

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, kafkaKey, kafkaMessage);

            // Send the record
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        log.info("Received message is : " + "\n" +
                                "Topic : " + metadata.topic() + "\n" +
                                "Partition : " + metadata.partition() + "\n" +
                                "Offset : " + metadata.offset() + "\n" +
                                "Value : " + producerRecord.value());
                    }
                }
            });


            try{
                Thread.sleep(2000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }

            producer.flush();

        }
        // Flush and close the producer
        producer.close();
    }
}
