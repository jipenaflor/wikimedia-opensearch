package org.jipenaflor.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "172.18.161.170:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // high throughput at the expense of a bit of latency and CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // time to wait until we send a batch
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        /*
        Note: Kafka versions >= 2.4 are safe producers by default, i.e., acks = all, retries = max int,
        max.in.flight.requests.per.connection = 5 messages batches (has to do with parallel processing and batching),
        enable.idempotence (prevent duplicates)
        - replication factor = 3, acks = -1 (by all in-sync replicas), min.insync.replicas = 2
        is the guaranteed setup for data durability and availability because it can handle
        one broker going down
        - If producer sends faster than what the broker can receive, records will be buffered in the memory
        (32 mb). If the buffer gets full, send() will be blocked. If it remains so and
        max.block.ms has elapsed, exception will be thrown.
        */

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        // handle the events from the streams and send it to the producer
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // produce for 3 minutes then block the program
        TimeUnit.MINUTES.sleep(3);
    }
}
