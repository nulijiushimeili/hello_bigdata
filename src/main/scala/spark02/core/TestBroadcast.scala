package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量的使用
  * broadcast
  */
object TestBroadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("testBroadcast")
      .setMaster("local")
    val sc = new SparkContext(conf)
    //添加一个累加器
    val countor = sc.longAccumulator
    //创建广播变量
    val list = List("hadoop", "spark02")
    val broadcast = sc.broadcast(list)
    println(broadcast.value)
    //对map操作进行计数
    val rdd = sc.parallelize(Array(1,2,3,4,5,6)).map{x=>
      countor.add(1)
      (x,1)
    }
    rdd.foreach(println)
    println(countor)
  }
}
