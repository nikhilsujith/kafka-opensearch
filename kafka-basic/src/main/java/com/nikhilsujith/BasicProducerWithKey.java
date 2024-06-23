package com.nikhilsujith;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class BasicProducerWithKey {
    //  Add logging
    private static final Logger log = LoggerFactory.getLogger(BasicProducerWithKey.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting Producer");

//      Producer Properties

//        Connection properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "pkc-921jm.us-east-2.aws.confluent.cloud:9092"); // Confluent cloud
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='GWR2Z6YRG5XRVJAG' password='Nwv/yhcjB2IMZSE3V846JkxzPeZ6sUogK3qHsxc/Tm0ty+rNNZTt631hPxvBQHct';");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("client.dns.lookup", "use_all_dns_ips");

//        producer properties
        properties.setProperty("acks", "all");
        properties.setProperty("key.serializer", StringSerializer.class.getName()); // producer expecting strings
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("partitioner.class","org.apache.kafka.clients.producer.RoundRobinPartitioner");


//        Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0,2)
                .forEach(x -> {
                    IntStream.range(0, 10)
                            .forEach(index -> {
                                String topic = "SecondTopic";
                                String key = "id_" + index;
                                String value = "SecondTopic" + index;

//                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("DemoJava",String.valueOf(index));
                                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                                producer.send(producerRecord, new Callback() {
                                    @Override
                                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                        if (e!=null){ // e is not null if an exception is raised
                                            log.error("Error while producing message: ", e);
                                        }
                                        else{ // e is null if no exception
                                            log.info("Topic: "+recordMetadata.topic()
                                                    +"\tPartition: "+recordMetadata.partition()
                                                    +"\tKey: "+key
                                                    +"\tOffset: "+recordMetadata.offset()
                                                    +"\tTimestamp: "+recordMetadata.timestamp());
                                        }
                                    }
                                });
                            });
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

//        flush tells producer to send all data, but block until done. Synchronous send
//        producer.close() will call producer.flush
        producer.flush();

//        Close
        producer.close();
    }
}
