package kafka.twitter.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private final static String CONSUMER_KEY = "CONSUMER_KEY";
    private final static String CONSUMER_SECRET = "CONSUMER_SECRET";
    private final static String TOKEN = "TOKEN";
    private final static String TOKEN_SECRET = "TOKEN_SECRET";
    private final static Long FIVE_SECOND_POLL = 5L;
    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

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

        // on a different thread, or multiple different threads....
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
            }
        }

        client.stop();
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /**
         *  Connection Properties
         * */

        /** Declaring the host  the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("donald j trump", "donald trump", "Donald Trump");
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
