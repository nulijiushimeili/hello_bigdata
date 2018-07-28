// scalastyle:off println
package spark01.streaming



import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.spark.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */
object KafkaWordCount {
  def main(args: Array[String]) {

    val zkQuorum = "bigdata-senior02.ibeifeng.com:2181"
    val group = "g1"
    val topics = "orderTopic"
    val numThreads = 2

    val conf= new SparkConf().setAppName("StatelessWordCount").setMaster("local[2]")   // 核数至少给2,否则不会完成计算
    val ssc = new StreamingContext(conf,Seconds(2))  // 两秒进行一个批次

//    ssc.checkpoint("/checkpoint")
//
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
//    wordCounts.print()

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val wc = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map(_._2)
        .flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .foreachRDD(x=>x.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }
}

