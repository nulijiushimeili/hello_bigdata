package spark03.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by nulijiushimeili on 2018-07-27
  */
object SimpleWorldCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("world count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //    testRDD(sc)


    sc.stop()
  }

  /**
    * test rdd and use parallelize() load data.
    *
    * @param sc SparkContext
    */
  def testRDD(sc: SparkContext): Unit = {
    val list1 = List(1, 2, 3, 4, 5)
    val rdd = sc.parallelize(list1)
    val rdd1 = rdd.map(_ + 1).filter(_ > 3).map((_, 1))
    rdd1.foreach(println)

    sc.stop()
  }

  /**
    * Use textfile(path) load data and test spark rdd.
    *
    * @param sc   SparkContext
    * @param path String of file path.
    */
  def testRDD2(sc: SparkContext, path: String): Unit = {
    val fileRDD = sc.textFile(path)
    println("Count rdd: " + fileRDD.count())
    println("First rdd: " + fileRDD.first())
    val res = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortByKey()
    res.foreach(println)
  }

  /**
    * world count application
    *
    * @param sc SparkContext
    */
  def testRDD3(sc: SparkContext): Unit = {
    val arr = Array("abc abc d d a c ff a ff ", "zzz zzz ee dd cc cc ee dd ",
      "abc abc d d a c ff a ff ", "zzz zzz ee dd cc cc ee dd ")
    val rdd = sc.parallelize(arr).flatMap(_.split(" "))
    val wc = rdd.map((_, 1)).reduceByKey(_ + _).sortByKey()
    wc.foreach(println)
  }

  /**
    * Test pair rdd.  pairRDD 可以通过其他的rdd转化得到
    *
    * @param sc SparkContext
    */
  def testPairRDD(sc: SparkContext): Unit = {
    // parallelize(这里只能接受Seq的子类的数据类型)
    val rdd = sc.parallelize(List(1, 2, 3, 1, 3, 4, 4, 4, 5)) // 单元素
    rdd.foreach(println)
    println("---------------------------------------")

    val pairRDD = rdd.map((_, 1)).reduceByKey(_ + _) // pairRDD 中的每一个元素都是二元组,这样才能够方便运算
    pairRDD.foreach(println)
  }


  /**
    * 对pairRDD的操作:
    * 对key和value进行操作
    * 单独对value进行操作
    *
    * @param sc SparkContext
    */
  def pairRDDOperation(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(List(3, 2, 3, 4, 4, 5))

    val pairRDD = rdd.map(x => {
      if (x % 2 == 0) {
        (x, x + 2)
      } else {
        (x, x + 1)
      }
    }).persist(StorageLevel.MEMORY_ONLY)
    pairRDD.foreach(println)
    println("-------------------------------------")

    // 对rdd的key和value进行操作
    val mapRDD = pairRDD.map(x => (x._1 + 1, x._2 + 2))
    mapRDD.foreach(println)
    println("-------------------------------------")
    //    val mapRDD= pairRDD.filter{case (x,y)=> x > 4}    // 这种写法等同于上面的写法

    // 只对rdd的value进行操作
    val mapRDD2 = pairRDD.mapValues(_ + 2) // key 保持不变,value + 2
    mapRDD2.foreach(println)

  }


}
