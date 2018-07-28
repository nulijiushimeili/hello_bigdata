package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

case class SecondSorted(first: Int,second: Int)
  extends Ordered[SecondSorted] with Serializable {
  override def compare(that: SecondSorted): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}

object SecondSorted {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SecondSorted").setMaster("local[*]"))
    val lines = sc.textFile(
      "D:\\mycode1\\program\\spark\\sparksql\\file\\secondarySort.txt")
      .flatMap(_.split("\n"))
    val mapRDD = lines.map(x => (
      new SecondSorted(x.split(",")(0).toInt, x.split(",")(1).toInt), x))
    val res = mapRDD.sortByKey().map(x=> x._2)
    res.foreach(println)

  }
}