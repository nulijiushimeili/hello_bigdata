package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\hbase_data.txt"
    val file = sc.textFile(path)
    val lines = file.flatMap(_.split(" "))
    val wordRDD = lines.map((_, 1))
    val wordCount = wordRDD.reduceByKey(_+_)
    wordCount.collect.foreach(println)

    sc.stop()
  }
}
