package spark02.core

/**
  * textFile()提取出来的数据是文件中的每一行
  */

import org.apache.spark.{SparkConf, SparkContext}

object SecondColumeSorted {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("GetSecondColume"))
    val file = sc.textFile("D:\\mycode1\\program\\spark\\sparksql\\file\\lris.txt")
    val secondColume = file.map(_.split(",")).map(x=>x(1)).map((_,1))
    val sortedRes = secondColume.sortByKey(ascending = false)
    sortedRes.foreach(println)



  }
}
