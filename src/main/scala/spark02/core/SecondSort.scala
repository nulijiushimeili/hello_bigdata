package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序
  */
class SecondSort(val first: Int, val second: Int) extends Ordered[SecondSort] with Serializable {
  override def compare(that: SecondSort): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}

object SecondSort {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("SecondSort"))
    val lines = sc.textFile(
      "D:\\mycode1\\program\\spark\\sparksql\\file\\secondarySort.txt")
      .flatMap(_.split("\n"))
    val sortRes = lines.map(x => (new SecondSort(x.split(",")(0).toInt
      , x.split(",")(1).toInt),x)).sortByKey().map(x=> x._2)
    sortRes.foreach(println)

  }
}
