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
    private static String topic;
    private static String streamSourceUrl;

    KafkaWikimediaProducer(String topic, String streamSourceUrl) throws IOException {
       this.topic = topic;
       this.streamSourceUrl = streamSourceUrl;
       props = KafkaConfigUtil.getKafkaClientProps("creds/client.properties");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //  High Throuhput Producer Configs
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    }

    private void produceMessages(){
        //        Call Event Handler
        KafkaProducer<String, String> kafkaWikiProducer = new KafkaProducer<>(props);
        EventHandler eventHandler = new WikimediaChangeHandler(topic, kafkaWikiProducer);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(streamSourceUrl));
        EventSource eventSource = builder.build();
//        Start producer in another thread
        eventSource.start();
    }



    public static void main(String[] args) throws InterruptedException, IOException {

        String topic = "wikimedia.recentchange";
        String wikiDataSourceUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        KafkaWikimediaProducer kafkaWikimediaProducer = new KafkaWikimediaProducer(topic, wikiDataSourceUrl);
        kafkaWikimediaProducer.produceMessages();

//        Block main thread for 10 minutes; If not program will produce for ever?
        TimeUnit.MINUTES.sleep(10);
    }


}
