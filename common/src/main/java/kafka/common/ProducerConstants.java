package kafka.common;

public interface ProducerConstants {

    String ENABLE_IDEMPOTENCE_CONFIG_TRUE = "true";
    String ACKS_CONFIG_ALL = "all";
    String RETRIES_MAX_INTEGER = Integer.toString(Integer.MAX_VALUE);
    String COMPRESSION_TYPE_SNAPPY = "snappy";
    String LINGER_MS_5 = Integer.toString(20);
    String BATCH_SIZE_32KB = Integer.toString(32 * 1024);
}
