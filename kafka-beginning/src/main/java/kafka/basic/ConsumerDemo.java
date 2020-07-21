package kafka.basic;

import kafka.common.ConsumerUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);


        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-consumer-application";
        String topic = "first_topic";


        HashMap<String, Optional<String>> propertiesKeysWithValues = new HashMap<>();
        propertiesKeysWithValues.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Optional.ofNullable(bootstrapServers));
        propertiesKeysWithValues.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));
        propertiesKeysWithValues.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));
        propertiesKeysWithValues.put(ConsumerConfig.GROUP_ID_CONFIG, Optional.ofNullable(groupId));
        propertiesKeysWithValues.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Optional.ofNullable("earliest"));

        // create consumer configs
        Properties properties = ConsumerUtils.consumerUtils(propertiesKeysWithValues);

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }


    }
}
