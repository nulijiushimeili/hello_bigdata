package spark02.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 多个作业同时消费同一个topic时,
  *   1.每个作业拿到的是完整的数据,这是最常用的场景
  *   2.每个做个拿到一部分数据,相当于进行了负载均衡
  *   当多个作业的groupid相同时,属于第一种情况
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val zkQuorum = "bigdata-senior02.ibeifeng.com:2181"
    val group = "g1"
    val topics = "logTopic"
    val numThreads = 2

    val conf = new SparkConf().setAppName("streaming word count").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(2))
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map(_._2)
    val wc = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wc.print(3)
    wc.foreachRDD(_.foreach(println))


  }
}
