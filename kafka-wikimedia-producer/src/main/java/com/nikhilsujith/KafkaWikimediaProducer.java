package com.nikhilsujith;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaWikimediaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaWikimediaProducer.class.getSimpleName());
    private static Properties props;

    public static void main(String[] args) throws InterruptedException, IOException {
        props = KafkaConfigUtil.getKafkaClientProps("creds/client.properties");
        String topic = "wikimedia.recentchange";
        String wikiDataSourceUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        Call Event Handler
        KafkaProducer<String, String> kafkaWikiProducer = new KafkaProducer<>(props);
        EventHandler eventHandler = new WikimediaChangeHandler(topic, kafkaWikiProducer);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(wikiDataSourceUrl));
        EventSource eventSource = builder.build();

//        Start producer in another thread
        eventSource.start();

//        Block main thread for 10 minutes; If not program will produce for ever?
        TimeUnit.MINUTES.sleep(10);
    }


}
