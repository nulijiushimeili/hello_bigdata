package spark02.core

/**
  * 普通seq rdd的计算操作
  */

import org.apache.spark.{SparkConf, SparkContext}

object TestAction {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("testAction"))
    val rdd = sc.parallelize(1 to 10)
    val count = rdd.count()
    val first = rdd.first()
    val take: Array[Int] = rdd.take(2)
    val top: Array[Int] = rdd.top(3)

    println(count)       //计算rdd的个数
    println(first)       //
    take.foreach(println)
    top.foreach(println)   //默认是降序排列的

    val takeAsc = rdd.takeOrdered(3)     //升序获取前n个
    takeAsc.foreach(println)

    val getSum = rdd.sum()     //得到的结果是double类型的
    println(getSum)

    val getReduce = rdd.reduce(_+_)      //结果是整型
    println(getReduce)

    val getFold = rdd.fold(0)(_+_)   //在求和之前可以给定一个初始值
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
