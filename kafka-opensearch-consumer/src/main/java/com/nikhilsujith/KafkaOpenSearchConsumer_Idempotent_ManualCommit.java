/****
 *
 * Manually commit offsets after batch has processed
 *
 *
 *
 * */

package com.nikhilsujith;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class KafkaOpenSearchConsumer_Idempotent_ManualCommit {

    private static Logger log = LoggerFactory.getLogger(KafkaOpensearchConsumer.class.getName());
    Properties properties;
    private String topic;
    private String consumerGroup;
    private RestHighLevelClient openSearchClient = OpenSearchClient.createOpenSearchClient();

    KafkaOpenSearchConsumer_Idempotent_ManualCommit(String topic, String consumerGroup) throws IOException {
        log.info("Loading client properties");
        properties = KafkaConfigUtil.getKafkaClientProps("creds/client.properties");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.topic = topic;
        this.consumerGroup = consumerGroup;

    }

    private void createOpenSearchIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");

//        Check if index already exists
        boolean indexExists = openSearchClient
                .indices()
                .exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

//        If index does not exists, create a new one
        if (! indexExists){
            openSearchClient
                    .indices()
                    .create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("Wikimedia index created");
        } else {
            log.info("Wikimedia index already exists");
        }
    }

    private String getMessageId(String json){
        return JsonParser.parseString(json)
                .getAsJsonObject()
                    .get("meta")
                .getAsJsonObject()
                    .get("id")
                .getAsString();
    }

    private BulkRequest insertDataOpenSearch(ConsumerRecord consumerRecord, BulkRequest bulkRequest) throws IOException {

        String kafkaMessageId = getMessageId(consumerRecord.value().toString());
        IndexRequest indexRequest = new IndexRequest("wikimedia")
                .source(consumerRecord.value(), XContentType.JSON)
                .id(kafkaMessageId);
//        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
        BulkRequest br = bulkRequest.add(indexRequest);
        return br;

    }

    private void startConsumer(){
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        final Thread currentThread = Thread.currentThread(); // Reference to current thread
        /***
         * Shutdown hook listens for interrupts
         *
         * */
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                log.info("SHUTDOWN detected, going to gracefully shutdown kafka consumer...");
                kafkaConsumer.wakeup();
                try{
                    currentThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try{
//            Subscribe to a topic
            kafkaConsumer.subscribe(Arrays.asList(topic));

//            Poll a topic
            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
                BulkRequest bulkRequest = new BulkRequest();
                if (consumerRecords.count() > 0){
                    log.info("Received " + consumerRecords.count() + " from Kafka");

                    for (ConsumerRecord<String, String> record: consumerRecords){
                        log.info("key: " + record.key() +
                                "\tValue: " + record.value() +
                                "\tTopic: " + record.topic() +
                                "\tOffset: " + record.offset()
                        );

//                    Send data into opensearch
//                    Create an index request
                        try {
//                            bulkRequest = insertDataOpenSearch(record, bulkRequest);
                            String kafkaMessageId = getMessageId(record.value().toString());
                            IndexRequest indexRequest = new IndexRequest("wikimedia")
                                    .source(record.value(), XContentType.JSON)
                                    .id(kafkaMessageId);
                            bulkRequest.add(indexRequest);

                        } catch (Exception e){
                            log.warn("Unable to insert record into openSearch: ");
                            e.printStackTrace();
                        }
                    }

                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s).");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // commit offsets after the batch is consumed
                    kafkaConsumer.commitSync();
                    log.info("Offsets have been committed!");

                }else{
                    log.info("Waiting for new records from Kafka");
                }
            }
        } catch (WakeupException we){
            log.info("Consumer starting to shutdown "+ we);
        } catch (Exception e){
            log.error("Got unexpected exception:", e);
        } finally {
            kafkaConsumer.close();
        }

    }

    public static void main(String[] args) throws IOException {
        String topic = "wikimedia.recentchange";
        String cg = "consumer-group-3";
        KafkaOpenSearchConsumer_Idempotent_ManualCommit kosc = new KafkaOpenSearchConsumer_Idempotent_ManualCommit(topic, cg);
        kosc.createOpenSearchIndex();
        kosc.startConsumer();
    }
}
