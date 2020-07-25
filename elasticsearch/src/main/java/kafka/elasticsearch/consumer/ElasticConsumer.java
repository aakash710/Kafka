package kafka.elasticsearch.consumer;

import com.google.gson.JsonParser;
import kafka.common.ConsumerUtils;
import kafka.elasticsearch.configuration.ElasticsearchConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

import static kafka.common.ConsumerConstants.AUTO_OFFSET_EARLIEST;
import static kafka.elasticsearch.utils.ElasticConstants.*;

public class ElasticConsumer {

    private static Properties properties;
    private static JsonParser jsonParser = new JsonParser();
    Logger logger = LoggerFactory.getLogger(ElasticConsumer.class.getName());
    RestHighLevelClient client = ElasticsearchConfiguration.createClient();

    public ElasticConsumer() {
        HashMap<String, Optional<String>> propertiesKeysWithValues = new HashMap<>();
        propertiesKeysWithValues.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Optional.ofNullable(bootstrapServers_9092));
        propertiesKeysWithValues.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));//StringDeserializer.class
        propertiesKeysWithValues.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));
        propertiesKeysWithValues.put(ConsumerConfig.GROUP_ID_CONFIG, Optional.ofNullable(DONALD_TWEETS_GROUP_ID));
        propertiesKeysWithValues.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Optional.ofNullable(AUTO_OFFSET_EARLIEST));
        // create consumer configs
        properties = ConsumerUtils.consumerUtils(propertiesKeysWithValues);
    }

    public static void main(String[] args) {
        new ElasticConsumer().execute();
    }


    private static String extractIdFromTweet(String tweetJson) {
        // gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private void execute() {

        KafkaConsumer<String, String> elasticConsumer = new KafkaConsumer<String, String>(properties);

        elasticConsumer.subscribe(Arrays.asList(DONALD_TWEETS));

        while (true) {
            ConsumerRecords<String, String> records =
                    elasticConsumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {

                // 2 strategies
                // kafka generic ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed specific id
                try {
                    String id = extractIdFromTweet(record.value());

                    // where we insert data into ElasticSearch

                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .source(record.value(), XContentType.JSON)
                            .id(id); // this is to make our consumer idempotent

                    bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data: " + record.value());
                }

            }

            if (recordCount > 0) {
                try {
                    BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                logger.info("Committing offsets...");
                elasticConsumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close the client gracefully
        // client.close();
    }
}

