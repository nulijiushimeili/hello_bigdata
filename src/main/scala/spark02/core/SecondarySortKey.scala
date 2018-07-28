package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序的排序规则
  */
class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}

object SecondarySortKey {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("SecondarySortKey"))
    val file = sc.textFile(
      "D:\\mycode1\\program\\spark\\sparksql\\file\\secondarySort.txt", 4)
    //获取每一行的数据
    val lines = file.flatMap(_.split("\n"))
    //使用二次排序规则进行排序
    val numbers = lines.map(x =>
      (new SecondarySortKey(x.split(",")(0).toInt, x.split(",")(1).toInt), x)
    )
    val res = numbers.sortByKey().map(x => x._2)
    res.foreach(println)


  }
}


