package com.nikhilsujith;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class BasicConsumer {
    //  Add logging
    private static final Logger log = LoggerFactory.getLogger(BasicConsumer.class.getSimpleName());


    public static void main(String[] args) {
        log.info("Starting Consumer");

        String groupId = "java-consumer-app";
        String topic1 = "basic_topic";

//      Producer Properties

//        Connection properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092"); // Confluent cloud
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='GWR2Z6YRG5XRVJAG' password='Nwv/yhcjB2IMZSE3V846JkxzPeZ6sUogK3qHsxc/Tm0ty+rNNZTt631hPxvBQHct';");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("client.dns.lookup", "use_all_dns_ips");

//        Create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");

//        Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//        Subscribe to topicS
        consumer.subscribe(Arrays.asList(topic1));

//      Pool for data
        while (true){
            log.info("Polling");
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000); // If there is data will return immediately. If no data, wait 1 second to receive.

//        Extract data from consumer record
            for (ConsumerRecord<String, String> record: consumerRecords){
                log.info("Key: "+record.key()
                        +"\tValue: "+record.value()
                        +"\tPartition: "+record.partition()
                        +"\tTopic: "+record.topic()
                        +"\tOffset: "+record.offset());
            }

        }
    }
}
