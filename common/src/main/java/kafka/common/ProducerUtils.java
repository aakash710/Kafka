package kafka.common;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

import static kafka.common.ProducerConstants.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public interface ProducerUtils {

    static Properties producerUtils(HashMap<String, Optional<String>> propertiesKeysWithValues) {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, propertiesKeysWithValues.get(BOOTSTRAP_SERVERS_CONFIG).orElse("127.0.0.1:9092"));
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, propertiesKeysWithValues.get(KEY_SERIALIZER_CLASS_CONFIG).orElse(StringSerializer.class.getName()));
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, propertiesKeysWithValues.get(VALUE_SERIALIZER_CLASS_CONFIG).orElse(StringSerializer.class.getName()));
        return properties;
    }

    static Properties defaultProducerUtils() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    static Properties enableSafeProperties(Properties properties) {
        //enable safe properties
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_IDEMPOTENCE_CONFIG_TRUE);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG_ALL);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, RETRIES_MAX_INTEGER);
        return properties;
    }



}
