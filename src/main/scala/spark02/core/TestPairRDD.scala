package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  *  pair RDD     键值对RDD
  *
  */
object TestPairRDD {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("testPairRDD").setMaster("local"))
    val list = List(4,5,76,2,6,3,9,55,5,5,67,34,6,4,34,3,43,45,23,5,5,5,5,5)
    val rdd = sc.parallelize(list).map((_,1))
    //对每个value值进行操作
    val rdd2 = rdd.mapValues(x=>x+10)
//    rdd2.foreach(println)
    // flatMapValues(x=> x to 10) 将每个Key map成value从1-10个将原来的数据集翻了10倍
    val rdd3 = rdd.flatMapValues(x=>x to 5)
//    rdd3.foreach(println)

    //类似于reduceByKey()
    val rdd4 = rdd.countByKey()
   // rdd4.foreach(println)

    //得到的是不重复的value为1的键值对
    val rdd5 = rdd.collectAsMap()
//    rdd5.foreach(println)

    // 给定一个key,查看key所对应的value 序列
    val rdd6 = rdd.lookup(5)
    println(rdd6)

    sc.stop()
  }
}
