package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器
  */
/**
  * 更新广播变量(rebroadcast)
  * 广播变量可以用来更新一些大的配置变量，比如数据库中的一张表格，那么有这样一个问题，
  * 如果数据库当中的配置表格进行了更新，我们需要重新广播变量该怎么做呢。
  * 上文对广播变量的说明中，我们知道广播变量是只读的，也就是说广播出去的变量没法再修改，
  * 那么我们应该怎么解决这个问题呢？
  * 答案是利用spark中的unpersist函数
  *
  * 基本思路：将老的广播变量删除（unpersist），然后重新广播一遍新的广播变量，
  * 为此简单包装了一个用于广播和更新广播变量的wraper类
  */
object Accumulators {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test accumulators")
    val sc = new SparkContext(conf)

    // 定义一个累加器
    val accum = sc.longAccumulator("my Accumulator")
    sc.parallelize(Array(1,2,3,4)).foreach(accum.add(_))

    println(accum.value)


    // 定义一个广播变量
    val broadcastVar = sc.broadcast(Array(1,2,3))
    println(broadcastVar.value)

  }
}
