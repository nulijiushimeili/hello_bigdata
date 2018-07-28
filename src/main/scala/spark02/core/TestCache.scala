package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object TestCache {
  def main(args: Array[String]): Unit = {
    val list = List("spark02", "hbase", "hive")
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("testCache"))
    val listRdd = sc.parallelize(list)
    val rdd2 = listRdd.flatMap(_.split(" ")).map((_, 1))
    //在没有触发action操作之前,系统不会立马进行缓存
    rdd2.cache()
    val count = rdd2.count()
    println(count)
    val wc = rdd2.reduceByKey(_ + _)
    println(wc)
    //在调用了cache()方法后实际上是调用了persist(MEMORY_ONLY)这个方法
    //使用缓存后,rdd的计算就不会从头开始了,在数据量大的时候可以避免重复计算,提高运行的效率,
    val take_1 = rdd2.collect().take(1)
    println(take_1)


  }
}
