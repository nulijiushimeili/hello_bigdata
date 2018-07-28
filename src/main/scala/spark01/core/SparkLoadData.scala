package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkLoadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark load data")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val wcFormFile = sc.textFile("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\wc.input")
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .reduceByKey(_ + _)

    wcFormFile.collect.foreach(println)

    val list = List(0 to 20)

    val wcParallelize = sc.parallelize(list)
      .flatMap(x => x)
      .map((_, 1))
      .sortByKey()
      .filter(x => x._1 > 10)

    wcParallelize.collect.foreach(println)


    sc.stop()
  }
}
