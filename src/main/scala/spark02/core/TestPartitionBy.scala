package spark02.core

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object TestPartitionBy {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("TestPartitionBy"))
    /**
      * 读取文件并对文件进行分区
      */
    val rdd = sc.textFile("D:\\mycode1\\program\\spark\\sparksql\\file\\data.txt",4)
    val lines = rdd.flatMap(_.split("\n"))
    val numbers = lines.flatMap(_.split("    ").reverse.take(1).map((_,1)))
    numbers.foreach(println)
    /**
      * 在集群运行的时候分区容易使数据不够精确
      * partitionBy(new HashPartitioner(1))  重置分区
      */
    val res = numbers.partitionBy(new HashPartitioner(1)).sortByKey().take(10)
    res.foreach(println)
  }
}
