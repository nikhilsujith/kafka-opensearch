package com.nikhilsujith;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class BasicConsumerWithGracefulShutdown {
    //  Add logging
    private static final Logger log = LoggerFactory.getLogger(BasicConsumerWithGracefulShutdown.class.getSimpleName());


    public static void main(String[] args) {
        log.info("Starting Producer");

        String groupId = "java-consumer-app";
        String topic1 = "SecondTopic";

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
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

//        Get reference to current thread
        final Thread mainThread = Thread.currentThread();

//        Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){ // Looks for an exit interrupt
            public void run(){
                log.info("Detected shutdown, exit by calling consumer.wakeup");
                kafkaConsumer.wakeup(); // causes an exception to be thrown on main thread operation (consumer.poll). Once the exception is thrown, call mainthread.join to wait for previous to finish execution

//          join main thread to allow exec of code in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try{
            //        Subscribe to topicS
            kafkaConsumer.subscribe(Arrays.asList(topic1));

//      Pool for data
            while (true){
                log.info("Polling");
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000); // If there is data will return immediately. If no data, wait 1 second to receive.
//        Extract data from consumer record
                for (ConsumerRecord<String, String> record: consumerRecords){
                    log.info("Key: "+record.key()
                            +"\tValue: "+record.value()
                            +"\tPartition: "+record.partition()
                            +"\tTopic: "+record.topic()
                            +"\tOffset: "+record.offset());
                }

            }
        } catch (WakeupException we){
            log.info("Consumer starting to shutdown "+ we);
        } catch (Exception e){
            log.error("Unexpected exception");
        } finally {
            kafkaConsumer.close();
            log.info("Consumer gracefully shutdown");
        }


    }
}
