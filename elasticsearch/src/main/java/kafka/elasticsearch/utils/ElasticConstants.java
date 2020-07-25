package kafka.elasticsearch.utils;

import com.google.common.collect.Lists;

import java.util.List;

public interface ElasticConstants {

    String DONALD_TWEETS = "donald_tweets";
    String DONALD_TWEETS_GROUP_ID = "donald_tweets_group";
    String bootstrapServers_9092 = "127.0.0.1:9092";
    List<String> TERMS = Lists.newArrayList("donald j trump", "donald trump", "Donald Trump");
}
