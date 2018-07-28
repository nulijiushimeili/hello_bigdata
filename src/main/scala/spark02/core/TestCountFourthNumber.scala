package spark02.core

/**
  * 在a.txt文件中获取其中一列的词频统计
  */

import org.apache.spark.{SparkConf, SparkContext}

object TestCountFourthNumber {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("TestCountFourthNumber"))
    val rdd = sc.textFile("D:\\mycode1\\program\\spark\\sparksql\\file\\a.txt")
    val linesRDD = rdd.flatMap(_.split("\n"))
    //获取最后一列的值的词频统计
    val wordsRDD = linesRDD.flatMap(_.split(",").reverse.take(1).map((_,1)))
//    wordsRDD.foreach(println)
    //获取第三列的词频统计
    val thirdColumeCount = linesRDD.flatMap(_.split(",").map((_,1)).take(3).reverse.take(1))
    thirdColumeCount.foreach(println)

  }
}
