package spark03.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * create by nulijiushimeili on 2018-08-01
  */
object VisitOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(s"${this.getClass.getName}")
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val fileRDD = spark.sparkContext
      .textFile("file:\\D:\\tmp\\data\\2015082818")
      .filter(line => line.trim.length > 0)
      .map { line =>
        val arr = line.split("\t")
        val date = arr(17).substring(0, 10)
        val guid = arr(5)
        val sessionId = arr(10)
        val url = arr(1)
        (date, guid, sessionId, url)
      }.filter(i => i._4.length > 0)
      .toDF("date", "guid", "sessionId", "url")
      .persist(StorageLevel.MEMORY_AND_DISK)

    fileRDD.createOrReplaceTempView("log")

    val sql_str =
      s"""
         |select date,count(distinct guid) uv,sum(pv) pv,
         |count(case when pv >= 2 then sessionId else null end) second_num,
         |count(sessionId) visits from
         |(select date ,sessionId, max(guid) guid, count(url) pv from log
         |group by date , sessionId) a
         |group by date
       """.stripMargin

    val res = sql(sql_str).cache()

    res.show()

    res.printSchema()

    spark.stop()

  }
}
