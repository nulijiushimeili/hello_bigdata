package spark01.core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}


class UserDefPartitioner(numParts:Int)extends Partitioner{
  // 覆盖分区数
  override def numPartitions: Int = numParts

  // 覆盖分区号获得函数
  override def getPartition(key: Any): Int = {
    key.toString.toInt%10
  }

}

object Test{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test_partitioner").setMaster("local[*]"))
    val data = sc.parallelize(1 to 10 ,5)
    val rdd = data.map((_,1)).partitionBy(new UserDefPartitioner(10))
      .map(_._1).saveAsTextFile("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\udf_partitioner_res")
  }

}
