package kafka.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ProducerDemoWithKeys {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);


        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-consumer-application";
        String topic = "first_topic";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

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
