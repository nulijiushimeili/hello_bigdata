package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object TestSortBy {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("TestSortBy"))
    val list = List("one","two","two","three","three","three")
    val rdd = sc.parallelize(list)
    /**
      * srotBy()可以通过value进行排序
      */
    val rdd2 = rdd.map((_,1)).reduceByKey(_+_)
    val rdd3 = rdd2.sortByKey(ascending = false)
    val rdd4 = rdd2.sortBy(_._2,ascending = false)
    rdd3.foreach(println)
    rdd4.foreach(println)


    sc.stop()
  }
}
