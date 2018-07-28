package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object TestJoinDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("join").setMaster("local"))
    val list1 = Array("java","hadoop","hive")
    val list2 = Array("hadoop","scala")
    val rdd1 = sc.parallelize(list1).map((_,1))
    val rdd2 = sc.parallelize(list2).map((_,1))
    /**
      * 将两个RDD数据集进行连接操作,生成(key,(v1,v2))的形势,
      * 只有当两个数据集中都有相同的key的时候才会有结果输出
      */
    val rdd3 = rdd1 join rdd2
    rdd3.foreach(println)       //(hadoop,(1,1))

    /**
      * mapValues()对RDD的value进行操作
      */
    val rdd4 = rdd1.mapValues(_+10)
    rdd4.foreach(println)
    sc.stop()
  }
}
