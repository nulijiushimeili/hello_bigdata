package spark01.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object VisitCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ReadMysql2")
      .config("spark.sql.warehouse.dir", "file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val fileRDD = spark.sparkContext
      .textFile("hdfs://bigdata-senior02.ibeifeng.com:8020/user/hive/warehouse/log=20160505")
      .filter(line => line.length > 0)
      .map { line =>
        val arr = line.split("\t") // hive表的默认分割符是 \001
      val date = arr(5).substring(0, 10)
        val guid = arr(3)
        val url = arr(1)
        (date, guid, url)
      }.filter(i => i._3.length > 0).persist(StorageLevel.DISK_ONLY)

    fileRDD.toDF().show()

    val uvRDD = fileRDD.map(line => (line._1 + "_" + line._2, 1)).groupByKey()
      .map { x =>
        val arr = x._1.split("_")
        (arr(0), 1)
      }.reduceByKey(_ + _)

    uvRDD.foreach(println)

    val pvRDD = fileRDD.map(i => (i._1, 1)).reduceByKey(_ + _)

    pvRDD.foreach(println)

    spark.stop()

  }
}
