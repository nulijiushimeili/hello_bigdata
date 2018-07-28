package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 结果有问题
  */
//case class Girl(faceValue: Int, age: Int)
//
//object MySort {
//  implicit val girlOrdering: Ordering[Girl] = new Ordering[Girl] {
//    override def compare(x: Girl, y: Girl): Int = {
//      if (x.faceValue != x.faceValue) {
//        x.faceValue - y.faceValue
//      } else {
//        if (x.age > y.age) y.age - x.age else x.age - y.age
//        //        y.age - x.age
//      }
//    }
//  }
//
//}
//
//
//object SecondarySort2 {
//  def main(args: Array[String]): Unit = {
//    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("secondary sort"))
//
//    val girlInfo = sc.parallelize(Array(("yangying", 80, 25), ("yangmi", 80, 27), ("baby", 90, 36)))
//
//    //    val res = girlInfo.sortBy(_._2)
//    //    res.foreach(println)
//
//    import MySort.girlOrdering
//    val res = girlInfo.sortBy(x => Girl(x._2, x._3))
//    res.foreach(println)
//  }
//}
