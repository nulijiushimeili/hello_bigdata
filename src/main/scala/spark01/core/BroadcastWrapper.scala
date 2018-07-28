package spark01.core

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
  * 设置一个更新广播变量的类
  * @param ssc  StreamingContext
  * @param _v   broadcast
  * @tparam T   broadcast class
  */
case class BroadcastWrapper [T:ClassTag](
  @transient private val ssc :StreamingContext,
  @transient private val _v:T){

  @transient private var v = ssc.sparkContext.broadcast(_v)

  def update (newValue:T,blocking:Boolean =false): Unit ={
    // 删除RDD是否需要锁定
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }

  def value :T = v. value

  private def writeObject(out:ObjectOutputStream): Unit ={
    out.writeObject(v)
  }

  private def readObject(in:ObjectInputStream): Unit ={
    v = in .readObject().asInstanceOf[Broadcast[T]]
  }
}

/**
  * 测试更新广播变量的类
  */
object TestBroadcast{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test rebroadcast")
      .master("local[*]")
      .getOrCreate()

      import spark.implicits._

    val ssc = new StreamingContext(spark.sparkContext,Seconds(30))
    val newBroadcast = BroadcastWrapper[Array[Int]](ssc,Array(13,344,5,6,7))

    val zkQuorum = "bigdata-senior02.ibeifeng.com:2181"
    val group = "g1"
    val topics = "orderTopic"
    val numThreads = 2

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val wc = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap).map{
      // 在这里定期更新广播变量
      newBroadcast.update(Array(34,34,34,343,43),blocking = true)
      _._2
    }
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .foreachRDD(x=>x.foreach(println))


    ssc.awaitTermination()
    ssc.start()
  }
}
