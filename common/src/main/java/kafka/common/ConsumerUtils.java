package kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public interface ConsumerUtils {

    static Properties consumerUtils(HashMap<String, Optional<String>> propertiesKeysWithValues) {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, propertiesKeysWithValues.get(BOOTSTRAP_SERVERS_CONFIG).orElse("127.0.0.1:9092"));
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, propertiesKeysWithValues.get(VALUE_DESERIALIZER_CLASS_CONFIG).orElse(StringDeserializer.class.getName()));
        properties.setProperty(GROUP_ID_CONFIG, propertiesKeysWithValues.get(GROUP_ID_CONFIG).orElse("my-default-group-app"));
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, propertiesKeysWithValues.get(AUTO_OFFSET_RESET_CONFIG).orElse("earliest"));
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, propertiesKeysWithValues.get(KEY_DESERIALIZER_CLASS_CONFIG).orElse(StringDeserializer.class.getName()));
        return properties;
    }

    static void defaultLogKeysAndValues(KafkaConsumer<String, String> consumer, Logger logger) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
            logger.info("Key: " + record.key() + ", Value: " + record.value());
            logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
        }
    }

}

