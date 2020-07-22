package kafka.basic;

import kafka.common.ConsumerUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

public class ConsumerDemoWithAssignAndSeek {

    static HashMap<String, Optional<String>> propertiesKeysWithValues = new HashMap<>();
    final static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignAndSeek.class);
    String bootstrapServers = "127.0.0.1:9092";
    final static String groupId = "my-consumer-application";
    final static String topic = "first_topic";
    protected static Properties properties;

    public ConsumerDemoWithAssignAndSeek() throws ConfigurationException {
        //constructor used to set resource file paths
        propertiesKeysWithValues.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Optional.ofNullable(null));
        propertiesKeysWithValues.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));//StringDeserializer.class
        propertiesKeysWithValues.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));
        propertiesKeysWithValues.put(ConsumerConfig.GROUP_ID_CONFIG, Optional.ofNullable(groupId));
        propertiesKeysWithValues.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Optional.ofNullable("earliest"));
        // create consumer configs
        properties = ConsumerUtils.consumerUtils(propertiesKeysWithValues);
    }

    public void execute() {
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offsetToReadFrom = 1;
        consumer.assign(Arrays.asList(topicPartition)); //assigned, now seek

        //seek
        consumer.seek(topicPartition, offsetToReadFrom);

        int numberOfMessagesToRead = 10;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        //poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesToRead--;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                if(numberOfMessagesToRead < 1) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws ConfigurationException {

        //poll the data
        new ConsumerDemoWithAssignAndSeek().execute();

    }
}
