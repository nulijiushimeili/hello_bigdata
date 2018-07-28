package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object TestRepartition {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("testRepartition").setMaster("local"))
    val file = sc.textFile(
      "D:\\mycode1\\program\\spark\\sparksql\\src\\main\\resources\\log4j.properties")
    //强行对rdd进行分区
    val rdd = file.repartition(4)
    println(rdd.getNumPartitions)
    val wc = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //wc.foreach(println)


  }
}
