package com.nikhilsujith;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class WikimediaChangeHandler implements EventHandler {

    private static Logger log = Logger.getLogger(WikimediaChangeHandler.class.getName());

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    private int delaySend = 10;
    private static int delayCounter = 0;
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

        /***
         * Producer send lambda version
         * (RecordMetatda, exception) -> {func}
         *
         * */
        kafkaProducer.send(
                new ProducerRecord<>(topic, messageEvent.getData()),
                (recordMetadata, e) -> {
                    delayCounter++;
                    if (delayCounter == 20){
                        try {
                            log.info("Sleeping for 5 seconds");
                            Thread.sleep(5000);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                        delayCounter = 0;
                    }
                    log.info("Sent: " + messageEvent.getData());
                    log.info("Topic: " + recordMetadata.topic() +
                            "\tOffset: " + recordMetadata.offset() +
                            "\tPartition: " + recordMetadata.partition() +
                            "\tTimestamp: " + recordMetadata.timestamp());
                }
        );

/*
// NON LAMBDA VERSION
            kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                log.info("Sent: " + messageEvent.getData());
                log.info("Topic: " + recordMetadata.topic() +
                        "\tOffset: " + recordMetadata.offset() +
                        "\tPartition: " + recordMetadata.partition() +
                        "\tTimestamp: " + recordMetadata.timestamp());
            }
        });*/
    }



    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {

    }
}
