package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object TestParallelize {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test_parallelize"))
    val list = List("hadoop","hive","java", "spark02")
    val lines = sc.parallelize(list)
    val words = lines.flatMap(_.split(","))
    val wordMap = words.map((_,1))
    wordMap.foreach(println)

//    val helloWorld = for(i <- words)yield("hello",i)
//    helloWorld.foreach(println)

    val list2 = List("numpy","pandas","matplotlib","pyspark")
//    val newRDD = for(i <- list;j <- list2)yield(i,j)
//    newRDD.foreach(println)
    val lines2 = sc.parallelize(list2)
    val words2 = lines.flatMap(_.split(","))


  }
}
