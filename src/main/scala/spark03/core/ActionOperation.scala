package spark03.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by nulijiushimeili on 2018-07-27
  */
/**
  * test spark action operation.
  */
object ActionOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val rdd = sc.parallelize(1 to 10)
    val rdd1 = rdd.persist(StorageLevel.MEMORY_ONLY)
    // StorageLevel.MEMORY_ONLY 和 rdd.cache() 作用是一样样的
    val rdd2 = rdd.cache()
    rdd1.count()
    rdd1.first()
    rdd1.take(3)
    rdd2.top(3) // default sort by desc


    val rdd3 = rdd2.sum() // return "Double" type
    println(rdd3)
    val rdd4 = rdd1.reduce(_ + _) // 将序列中的每一个值两两相加进行迭代计算
    println(rdd4)
    println(rdd1.fold(0)((x, y) => x + y)) // 给定一个初始值,将序列中的值两两相加进行迭代计算
    rdd1.unpersist()
    rdd2.unpersist()
  }
}
