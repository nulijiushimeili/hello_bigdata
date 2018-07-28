package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

object TestAction {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("TestAction").setMaster("local"))
    val rdd = sc.parallelize(1 to 10)
    val count = rdd.count()
    val first = rdd.first()
    val tack2 = rdd.take(2)
    val top3 = rdd.top(3)

    println(count)
    println(first)
    tack2.foreach(println)
    top3.foreach(println)

    val takeAsc3 = rdd.takeOrdered(3)
    takeAsc3.foreach(println)

    val getReduce = rdd.reduce(_+_)
    println(getReduce)

    val getSum = rdd.sum()
    println(getSum)

    val getFold = rdd.fold(100)(_+_)
    println(getFold)

    /**
      * 使用fold()进行带有初始值计算的时候,会每个分区都加一次,
      * 计算完汇总的时候也会再加一次初始值
      */
    val rdd2 = rdd.repartition(4)
    val getFold1 = rdd2.fold(10)(_+_)
    println(getFold1)

    sc.stop()

  }

}
