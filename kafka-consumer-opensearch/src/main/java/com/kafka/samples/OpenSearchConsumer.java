package com.kafka.samples;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServer  ="127.0.0.1:9092";
        String topic = "wikimedia";
        String groupId = "orjjavagrp";

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        return consumer;
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String conn = "http://localhost:9200";
        URI connUri = URI.create(conn);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(),connUri.getPort(), "http")));
        return restHighLevelClient;
    }

    public static String extractId(String json){
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        logger.info("Starting Kafka Consumer Client");
        // Get Kafka Consumer Client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        // Get OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        // 1. Create an OpenSearch Index if not exists
        GetIndexRequest getIndexRequest = new GetIndexRequest("wikimedia");
        boolean indexExists = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        // 2. Create index if response "false"
        if (!indexExists){
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            logger.info("Opensearch index is created");
        } else {
            logger.info("Index already exists");
        }
        //3. Subsribe to a topic from consumer
        kafkaConsumer.subscribe(Collections.singleton("wikimedia"));

        while (true) {
            ConsumerRecords<String, String> records =  kafkaConsumer.poll(Duration.ofMillis(3000));
            logger.info("Received messages  : " + records.count());
            for (ConsumerRecord<String, String> record: records){

                String id = extractId(record.value());
                try{
                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .id(id)
                            .source(record.value(), XContentType.JSON);
                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info("Loaded Document Id : " + indexResponse.getId());
                } catch (Exception e){
                    e.printStackTrace();
                }

            }
        }

    }
}
