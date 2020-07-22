package kafka.basic;

import kafka.common.BaseApplication;
import kafka.common.ConsumerUtils;
import kafka.common.PropertyUtils;
import org.apache.commons.configuration.ConfigurationException;
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

public class ConsumerDemo extends BaseApplication {

    static HashMap<String, Optional<String>> propertiesKeysWithValues = new HashMap<>();
    final static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    String bootstrapServers = "127.0.0.1:9092";
    final static String groupId = "my-consumer-application";
    final static String topic = "first_topic";
    protected static Properties properties;

    public ConsumerDemo() throws ConfigurationException {
        //constructor used to set resource file paths
        propertiesKeysWithValues.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Optional.ofNullable(null));
        propertiesKeysWithValues.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));//StringDeserializer.class
        propertiesKeysWithValues.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));
        propertiesKeysWithValues.put(ConsumerConfig.GROUP_ID_CONFIG, Optional.ofNullable(groupId));
        propertiesKeysWithValues.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Optional.ofNullable("earliest"));
        // create consumer configs
        properties = ConsumerUtils.consumerUtils(propertiesKeysWithValues);
    }

    @Override
    public void execute() {
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

    public static void main(String[] args) throws ConfigurationException {

        //poll the data
        new ConsumerDemo().execute();

    }
}
