package kafka.twitter.consumer;

import kafka.common.ConsumerUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

import static kafka.twitter.utils.TwitterConstants.*;

public class TwitterConsumer {
    private static Properties properties;
    private static Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);

    public TwitterConsumer() {
        HashMap<String, Optional<String>> propertiesKeysWithValues = new HashMap<>();
        propertiesKeysWithValues.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Optional.ofNullable(bootstrapServers_9092));
        propertiesKeysWithValues.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));//StringDeserializer.class
        propertiesKeysWithValues.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringDeserializer.class.getName()));
        propertiesKeysWithValues.put(ConsumerConfig.GROUP_ID_CONFIG, Optional.ofNullable(DONALD_TWEETS_GROUP_ID));
        propertiesKeysWithValues.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Optional.ofNullable("earliest"));
        // create consumer configs
        properties = ConsumerUtils.consumerUtils(propertiesKeysWithValues);
    }

    public static void main(String[] args) {
        new TwitterConsumer().execute();
    }

    private void execute() {
        //create twitter consumer
        KafkaConsumer<String, String> twitterConsumer = new KafkaConsumer<String, String>(properties);

        //subscribe
        twitterConsumer.subscribe(Arrays.asList(DONALD_TWEETS));

        //poll for new data
        while (true) {
            ConsumerUtils.defaultLogKeysAndValues(twitterConsumer, logger);
        }
    }
}
