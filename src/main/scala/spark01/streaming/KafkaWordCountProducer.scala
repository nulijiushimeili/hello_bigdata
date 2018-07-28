package spark01.streaming

import java.util
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


/**
  * 这是一个数据生产端
  *
  * 开启服务器,broker, 如果不开启这个会提示没有找到broker
  * bin/kafka-server-start.sh -daemon config/server.properties &
  *
  * 开启消费端
  * bin/kafka-console-consumer.sh --zookeeper bigdata-senior02.ibeifeng.com:2181 --topic orderTopic --from-beginning
  */

// Produces some random words between 1 and 100.
object KafkaWordCountProducer {

  def main(args: Array[String]) {

    val topic = "orderTopic"
    val brokers = "bigdata-senior02.ibeifeng.com:9092"
    val messagesPerSec = 10
    val wordsPerMessage = 5

    val props = new util.HashMap[String,Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }

}
// scalastyle:on println

