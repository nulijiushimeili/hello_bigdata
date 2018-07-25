package bigdata01.kafka;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class KafkaProducerConsumerTest {
    private static Logger logger = LogManager.getLogger(KafkaProducerConsumerTest.class.getName());
    public static void main(String[] args) {
        new JavaKafkaProducer("topic1",true).start();

        new JavaKafkaConsumer("topic1").start();
    }
}
