package kafka.common;

public interface ProducerConstants {

    String ENABLE_IDEMPOTENCE_CONFIG_TRUE = "true";
    String ACKS_CONFIG_ALL = "all";
    String RETRIES_MAX_INTEGER = Integer.toString(Integer.MAX_VALUE);
}
