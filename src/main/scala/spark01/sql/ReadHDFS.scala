package spark01.sql

import org.apache.spark.sql.SparkSession

object ReadHDFS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read HDFS")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val file = sc.textFile("hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wc.input")
    val rdd = file.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)

    rdd.foreach(println)

  }
}
