package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

object SecondColumnSorted {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("SecondColumnSorted")
    val sc = new SparkContext(sparkConf)

    val path = "file:\\D:\\mycode1\\program\\spark\\spark01\\data\\emp.txt"
    val file = sc.textFile(path)
    val secondColumnRDD = file.map(_.split(" ")).map(x=>x(1)).map((_,1))
    val sortSecondColumn = secondColumnRDD.sortByKey()
    sortSecondColumn.foreach(println)

    sc.stop()
  }
}
