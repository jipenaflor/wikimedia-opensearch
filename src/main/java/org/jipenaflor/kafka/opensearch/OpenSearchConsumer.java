package org.jipenaflor.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        // String connString = "http://localhost:9200";
        String connString = "https://sh9eiomq71:iba78br58q@kafka-course-2146998149.ap-southeast-2.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static KafkaConsumer<String, String> createKafkaConsumer () {
        String groupId = "consumer-opensearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.161.170:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest - end; none - exception if no offset found
        // properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    public static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject()
                .get("meta").getAsJsonObject()
                .get("id").getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // create an OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create Kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        // graceful shutdown: get a reference to the main thread then add shutdown hook
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown detected. Calling consumer.wakeup() to exit...");
                kafkaConsumer.wakeup();
                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // create an index
        try (openSearchClient; kafkaConsumer) {
            GetIndexRequest getIndexRequest = new GetIndexRequest("wikimedia");
            boolean isIndexExists = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            if (!isIndexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } else {
                log.info("The index already exists.");
            }

            // subscribe to topic
            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                int recordCtr = records.count();
                log.info("Received records: " + recordCtr);

                // for batching records since sending index requests one by one is inefficient
                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record: records) {
                    /*
                    Strategies to ensure idempotence of the consumer:
                    - define an ID using the record coordinates
                    eg. String id = record.topic() + record.partition() + record.offset();
                    - utilize the id, if it exists, of the record
                    */

                    String id = extractId(record.value());

                    // send record to OpenSearch
                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);    // id included so that opensearch does not provide one
                                        // also useful in updating the same record if processing fails

                    bulkRequest.add(indexRequest);
                    // IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    // log.info("ID " + indexResponse.getId() + " received");
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " records.");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    /*
                    at most once - commit as soon as the message is received, possibility of loss
                    at least once - commit after processing, if it goes wrong it will be read again;
                        possibility of duplicates so ensure that process is idempotent
                    */

                    // commit offset after the batch is consumed ("at least once" commit strategy)
                    kafkaConsumer.commitSync();
                    log.info("Offset has been committed.");
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down...");
        } catch (Exception e) {
            log.info("Unexpected exception in the consumer", e);
        } finally {
            kafkaConsumer.close(); // also commits offsets
            openSearchClient.close();
            log.info("The consumer is gracefully shut down...");
        }
    }
}
