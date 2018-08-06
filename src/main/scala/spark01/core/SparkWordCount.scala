package spark01.core

import org.apache.spark.{SparkConf, SparkContext}
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-05
  */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    // local file
    val path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\word.txt"
    val savePath = s"file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\${System.currentTimeMillis()}"

    // 1. Create SparkContext
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster(SparkProperties.master)

    val sc = new SparkContext(conf)

    // The characters need to filtered
    val list = List(".","?",",",":","'")
    // Broadcast data
    val broadcastList = sc.broadcast(list)

    // 2. Load data use sc.textFile(path) and transform data.
    val wcRDD = sc.textFile(path)
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map(word => {
        (broadcastList.value.foldLeft(word)((a,b) => a.replace(b, "")), 1)
      }).reduceByKey(_+_)

    // Save result.
    wcRDD.foreachPartition(iter => iter.foreach(println))
    wcRDD.saveAsTextFile(savePath)

    // Clear broadcast.
    broadcastList.unpersist(true)

    sc.stop()

  }
}
