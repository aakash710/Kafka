package kafka.twitter.utils;

import com.google.common.collect.Lists;

import java.util.ArrayList;

public interface TwitterConstants {

    String DONALD_TWEETS = "donald_tweets";
    String DONALD_TWEETS_GROUP_ID = "donald_tweets_group";
    String bootstrapServers_9092 = "127.0.0.1:9092";
    ArrayList<String> TERMS = Lists.newArrayList("donald j trump", "donald trump", "Donald Trump");
    String ENABLE_IDEMPOTENCE_CONFIG = "true";
    String ACKS_CONFIG_ALL = "all";
    String RETRIES_MAX_INTEGER = Integer.toString(Integer.MAX_VALUE);
}
