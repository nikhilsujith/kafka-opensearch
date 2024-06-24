package com.nikhilsujith;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class KafkaOpensearchConsumer {

    private static Logger log = LoggerFactory.getLogger(KafkaOpensearchConsumer.class.getName());
    Properties properties;
    private String topic;
    private String consumerGroup;

    KafkaOpensearchConsumer(String topic, String consumerGroup) throws IOException {
        log.info("Loading client properties");
        properties = KafkaConfigUtil.getKafkaClientProps("creds/client.properties");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    private void startConsumer(){
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

//        Get Current Thread
        final Thread currentThread = Thread.currentThread();

        /***
         * Shutdown hook runs on a seperate thread listening for interrupt
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
            kafkaConsumer.subscribe(Arrays.asList(topic));
            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, String> record: consumerRecords){
                    log.info("key: " + record.key() +
                            "\tValue: " + record.value() +
                            "\tTopic: " + record.topic() +
                            "\tOffset: " + record.offset()
                    );
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
        String cg = "consumer-group1";
        KafkaOpensearchConsumer kosc = new KafkaOpensearchConsumer(topic, cg);
        kosc.startConsumer();
    }
}
