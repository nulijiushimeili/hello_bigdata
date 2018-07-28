package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object TestGroupReduce {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName(s"TestGroupReduce").setMaster("local"))
    val list = List("one","two","two","three","three","three")
    val rdd = sc.parallelize(list).map((_,1))
    /**
      * 将rdd进行groupByKey()操作后,通过map()进行reduceByKey()操作
      */
    val rdd2 = rdd.groupByKey()
    rdd2.foreach(println)
    println("--------------------------------------")
    val rdk = rdd2.map(t => (t._1,t._2.sum)).sortByKey()
    rdk.foreach(println)

    sc.stop()
  }
}
