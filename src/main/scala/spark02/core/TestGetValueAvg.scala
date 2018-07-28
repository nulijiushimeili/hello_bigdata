package spark02.core

/**
  * 计算hadoop和spark平均出现的次数
  */

import org.apache.spark.{SparkConf, SparkContext}

object TestGetValueAvg {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("getValueAvg").setMaster("local"))
    val arr = Array(("hadoop", 3), ("spark02", 5), ("hadoop", 3), ("spark02", 5),("spark02", 4))
    val rdd = sc.parallelize(arr)
      .map(x => (x._1, x._2))
      .groupByKey()
      .map(t => (t._1, t._2.sum.toDouble / t._2.size))
    rdd.foreach(println)

    sc.stop()

  }
}
