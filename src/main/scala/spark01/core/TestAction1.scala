package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

object TestAction1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("TestAction"))
    val rdd = sc.parallelize(1 to 10)
    rdd.foreach(println)
    // 计算rdd的个数
    val count = rdd.count()
    // 返回第一个rdd
    val first = rdd.first()
    // 提取前三个rdd
    val tack = rdd.take(3)
    // 从大到小提取3个rdd
    val top = rdd.top(3)

    println(count)
    println(first)
    tack.foreach(println)
    top.foreach(println)

    val takeAsc = rdd.takeOrdered(3)
    takeAsc.foreach(println)

    val getSum = rdd.sum()
    println(getSum)

    val getReduce = rdd.reduce(_+_)
    println(getReduce)

    // 给一个rdd先加上一个初始值,再累加起来
    val getFold = rdd.fold(10)(_+_)
    println(getFold)

    val rdd2 = rdd.repartition(4)
    val getFold2 = rdd2.fold(10)(_+_)
    println(getFold2)

    sc.stop()

  }
}
