package kafka.basic;

import kafka.common.ProducerUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithKeys implements ProducerUtils {

    String bootstrapServer;

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);


        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-consumer-application";
        String topic = "first_topic";

        HashMap<String, Optional<String>> propertiesKeysWithValues = new HashMap<>();
        propertiesKeysWithValues.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Optional.ofNullable(null));
        propertiesKeysWithValues.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringSerializer.class.getName()));
        propertiesKeysWithValues.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Optional.ofNullable(StringSerializer.class.getName()));
        // create consumer configs
        Properties properties = ProducerUtils.producerUtils(propertiesKeysWithValues);

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        IntStream.range(1, 10).forEach( i -> {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", Integer.toString(i), "hello world " + i);

            // send data - asynchronous
            producer.send(record);

            // flush data
            producer.flush();
        });
        // flush and close producer
        producer.close();
    }
}
