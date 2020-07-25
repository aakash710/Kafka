package kafka.twitter.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import kafka.common.ProducerUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static kafka.twitter.utils.TwitterConstants.DONALD_TWEETS;
import static kafka.twitter.utils.TwitterConstants.TERMS;

public class TwitterProducer implements ProducerUtils {
    private final static String CONSUMER_KEY = "tmt7CXxU9XzDhtRetEFK7X2pL";
    private final static String CONSUMER_SECRET = "5YtS17mGt6Xi2PZw74Yua0ZONixGVz1yH6cksEV2fOnuTEI0i2";
    private final static String TOKEN = "812689340-r85cGoUT7hlDqSPTZs41U5fzLPigRgyx3KSDUwJ8";
    private final static String TOKEN_SECRET = "4LhBb6B44cuaUzCTDcojwnHqUfabD14cTZU0Xpc4zsbJU";
    private final static Long FIVE_SECOND_POLL = 5L;
    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private static Properties properties;

    public TwitterProducer() {
        properties = ProducerUtils.defaultProducerUtils();

        //enable safe properties
        ProducerUtils.enableSafeProperties(properties);

        ProducerUtils.highThroughputProducer(properties);
    }

    public static void main(String[] args) {
        new TwitterProducer().execute();
    }

    private void execute() {
        //Setting up your blocking queues
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //create client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // on a different thread, or multiple different threads....
        Integer key = 0;
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (!Objects.equals(msg, null)) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>(DONALD_TWEETS, Integer.toString(key), msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (!Objects.equals(e, null)) {
                            logger.error("error message " + e.getMessage() + "\n" + e.getStackTrace());
                        }
                    }
                });
                key++;
            }
        }

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done! shutting down");
        }));
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /**
         *  Connection Properties
         * */

        /** Declaring the host  the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = TERMS;
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET,
                TOKEN, TOKEN_SECRET);

        /* Creating client */
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Twitter-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
