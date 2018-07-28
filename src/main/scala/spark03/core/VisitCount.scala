package spark03.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by nulijiushimeili on 2018-07-27
  *
  * 访客统计
  *   计算uv 和 pv
  */

object VisitCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getName}")
    val sc = new SparkContext(conf)

    val hdfsFile = "hdfs://192.168.75.130:8020/user/hive/warehouse/track_log/ds=2015-08-28/hour=18/"
    val fileRDD = sc.textFile(hdfsFile)
      .filter(line => line.length > 0)
      .map { line =>
        val arr = line.split("\t") // hive 表的默认分隔符是 \001
      val date = arr(17).substring(0, 10)
        val guid = arr(5)
        val url = arr(1)
        (date, guid, url)
      }.filter(i => i._3.length > 0).persist(StorageLevel.DISK_ONLY)

    println(fileRDD.count() + "----" + fileRDD.first())

    val uvRDD = fileRDD.map(line => (line._1 + "_" + line._2, 1)).groupByKey() // distinct 操作
      .map { x =>
      val arr = x._1.split("_")
      (arr(0), 1) // date,1
    }.reduceByKey(_ + _)

    val pvRDD = fileRDD.map(x => (x._1, 1)).reduceByKey(_ + _)
    uvRDD.join(pvRDD) // .foreach(println)
      //  写DB、写HDFS、sql写Hive表 ...
      .saveAsTextFile("hdfs://192.168.75.130:8020/user/root/visitCount")

    sc.stop()


  }
}
