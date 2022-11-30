package com.kafka.samples;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String kafkaTopic;

    private static Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String kafkaTopic){
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
    }
    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
        logger.info("Kafka Prdocuer is closed");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        logger.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {

    }
}
