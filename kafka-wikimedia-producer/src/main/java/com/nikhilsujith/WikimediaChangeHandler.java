package com.nikhilsujith;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.logging.Logger;

public class WikimediaChangeHandler implements EventHandler {

    private static Logger log = Logger.getLogger(WikimediaChangeHandler.class.getName());

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(String topic, KafkaProducer<String,String> kafkaProducer){
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        kafkaProducer.send(new ProducerRecord<>(topic, "key1", messageEvent.getData()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                log.info("Sent: " + messageEvent.getData());
                log.info("Topic: " + recordMetadata.topic() +
                        "\tOffset: " + recordMetadata.offset() +
                        "\tPartition: " + recordMetadata.partition() +
                        "\tTimestamp: " + recordMetadata.timestamp());
            }
        });
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}
