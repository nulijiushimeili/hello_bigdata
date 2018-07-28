package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 假设有hadoop/spark两种书,每种书某天的销量如下:
  * ("hadoop",5),("spark",7)
  * ("hadoop",3),("spark",6)
  * 求每天的平均值
  */
object TestAvg {
  def main(args: Array[String]): Unit = {
    val list1 = List(("hadoop", 5), ("spark02", 7))
    val list2 = List(("hadoop", 3), ("spark02", 6))
    val conf = new SparkConf()
      .setAppName("testAvg")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(list1).map(t => (t._1, t._2))
    val rdd2 = sc.parallelize(list2).map(t => (t._1, t._2))
    val joinRDD = rdd1 join rdd2
//    val hadoop = joinRDD.take(1).map(x => (x._1, x._2._1 + x._2._2))
//    val spark = joinRDD.take(2).reverse.take(1).map(x => (x._1, x._2._1 + x._2._2))
//    hadoop.foreach(println)
//    spark.foreach(println)
    val res = joinRDD.map(x=> (x._1,x._2._1+x._2._2))
    res.foreach(println)

  }
}
