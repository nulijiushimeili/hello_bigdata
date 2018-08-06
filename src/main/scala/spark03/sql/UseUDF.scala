package spark03.sql


import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * spark sql use udf function
  *
  * create by nulijiushimeili on 2018-08-01
  */
object UseUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
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
        val url = arr(1)
        (date, guid, url)
      }.filter(i => i._3.length > 0).persist(StorageLevel.MEMORY_AND_DISK)

    // 开发一个getTopic的UDF
    spark.udf.register("getName", (url: String, regex: String) => {
      val p: Pattern = Pattern.compile(regex)
      val m: Matcher = p.matcher(url)
      if (m.find()) {
        m.group(0).split("/")(1).toLowerCase()
      } else {
        null
      }
    })

    // 统计所有活动页的流量
    fileRDD.toDF("date", "guid", "url").createOrReplaceTempView("log")

    val sqlStr =
      s"""select date,getName(url,'sale/[a-zA-Z0-9]+'),count(distinct guid),count(url) from log
         |where url like '%sale%' group by date,getName(url,'sale/[a-zA-Z0-9]+')
       """.stripMargin
    sql(sqlStr).rdd.foreach(println)

    spark.stop()

  }
}
