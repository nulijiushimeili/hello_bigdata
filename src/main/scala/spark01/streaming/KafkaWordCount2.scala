package spark01.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  */
object KafkaWordCount2 {
  def main(args: Array[String]): Unit = {

    val zkQuorum = "bigdata-senior02.ibeifeng.com:2181"
    val group = "g1"
    val topics = "orderTopic"
    val numThreads = 2

    val conf= new SparkConf().setAppName("StatelessWordCount").setMaster("local[2]")   // 核数至少给2,否则不会完成计算
    val ssc = new StreamingContext(conf,Seconds(2))  // 两秒进行一个批次

    // 设置有状态的检查点
    ssc.checkpoint("hdfs://192.168.227.200:8020/user/root/checkpoint/wordcount")


    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map(_._2)

    val words = lines .flatMap(_.split(" "))
    val wc = words.map((_,1))
    // 每个批次打印3行
    wc.print(3)

    val addFunc = (currValues:Seq[Int],preValueState: Option[Int])=>{
      // 通过spark内部的reduceByKey, 按key进行归约,
      // 然后这里传入可以当前批次的Seq,再计算key的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = preValueState.getOrElse(0)
      // 返回了累加后的结果,是一个Option[Int]类型
      Some(currentCount + previousCount)
    }

    // 对pairRDD里的每个key的values进行addFunc处理
    wc.updateStateByKey[Int](addFunc).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
