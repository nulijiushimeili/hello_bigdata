package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("wordCount")
        .setMaster("local")
    )
//    val path = "D:\\mycode1\\program\\spark\\sparksql\\src\\file\\content.txt"
    val path = "hdfs://bigdata-senior01.ibeifeng.com:8020/user/hive/warehouse/db_emp.db/emp/emp.txt"
    val res = sc.textFile(path)
        .flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey((x,y) => x + y)
        //.sortByKey()

    res.collect.foreach(println)
    //
//    val savePath = "hdfs://bigdata-senior01.ibeifeng.com:8020/datas/new"
//    res.saveAsTextFile(savePath)




    sc.stop()
  }
}
