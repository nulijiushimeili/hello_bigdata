package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object TestJoin {
  def main(args: Array[String]): Unit = {

    val list1 = List(("hadoop",1),("hive",3),("hadoop",3))
    val list2 = List(("hadoop",3),("hadoop",3),("spark02",4))
    val sc = new SparkContext(new SparkConf().setAppName("TestJoin").setMaster("local"))
    val rdd1 = sc.parallelize(list1).map(x=> (x._1,x._2))
    val rdd2 = sc.parallelize(list2).map(x=>(x._1,x._2))
    val joinRDD = rdd1 join rdd2
    val leftJoinRDD = rdd1 leftOuterJoin rdd2
    leftJoinRDD.foreach(println)
    //    (hive,(3,None))
//    (hadoop,(1,Some(3)))
//    (hadoop,(1,Some(3)))
//    (hadoop,(3,Some(3)))
//    (hadoop,(3,Some(3)))

    val rightJoinRdd = rdd1 rightOuterJoin rdd2




  }

}
