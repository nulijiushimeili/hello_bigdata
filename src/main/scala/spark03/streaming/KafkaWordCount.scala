package spark03.streaming

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-04
  *
  * Consumers messages from one or more topics in Kafka and does wordcount.
  *
  * Usage: KafkaWordCount <zkQuorum> <group> <topic> <numThreads>
  * <zkQuorum> is a list of on or more zookeeper servers that make quorum
  * <group> is the name of kafka consumer group
  * <topic> is a list of one or more kafka topics to consume from
  * <numThreads> is the number of threads the kafka consumer should use
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topic> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val conf = new SparkConf()
      .setMaster(SparkProperties.master)
      .setAppName(this.getClass.getName)
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wc = words.map((_, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wc.print

    ssc.start()
    ssc.awaitTermination()
  }

}

// Produces come random words between 1 and 100.
object KafkaWordCountProducer {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> <MessagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordPerMessage) = args

    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some message
    while (true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString).mkString(" ")
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
    }
  }
}
