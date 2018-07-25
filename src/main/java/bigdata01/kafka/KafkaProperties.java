package bigdata01.kafka;

public class KafkaProperties {
    public static final String TOPIC = "topic1";
    public static final String KAFKA_SERVER_URL = "bigdata-senior02.ibeifeng.com";
    public static final int KAFKA_SERVER_PORT =9092;
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;

    private KafkaProperties(){}
}