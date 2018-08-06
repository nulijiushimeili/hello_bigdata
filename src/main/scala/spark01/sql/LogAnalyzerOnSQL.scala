package spark01.sql

import org.apache.spark.sql.SparkSession
import spark01.log.ApacheAccessLog
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-06
  */
object LogAnalyzerOnSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master(SparkProperties.master)
      .appName(this.getClass.getName)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\access.log"

    /** Load data and transform to DataFrame. */
    val apacheAccessLogDF = spark.sparkContext.textFile(path)
      .filter(ApacheAccessLog.isValidateLogLine)
      .map(ApacheAccessLog.parseLogLine)
      .toDF()

    // register temporary view
    apacheAccessLogDF.createOrReplaceTempView("log")
    sql("select * from log limit 1").show()

    /**
      * 需求一：求contentsize的平均值、最小值、最大值
      */
    val resultDF = sql(
      """
        |select
        |avg(contentSize) as avg_size,
        |min(contentSize) as min_size,
        |max(contentSize) as max_size
        |from log
      """.stripMargin)

    resultDF.show()

    // Collect result
    val resRDD = resultDF.map(row => {
      val avgSize = row.getAs[Double]("avg_size")
      val minSize = row.getAs[Long]("min_size")
      val maxSize = row.getAs[Long]("max_size")
      (avgSize, minSize, maxSize)
    })

    // Save result
    resRDD.write.save(s"file:\\tmp\\${System.currentTimeMillis()}")
    resRDD.foreachPartition(iter => {
      // TODO : Save result.
      iter.foreach(println)
    })

    /**
      * 需求二：请各个不同返回值的出现的数据 ===> wordCount程序
      */
    val resDF2 = sql(
      """
        |select
        |responseCode as code,
        |count(1) as count
        |from log
        |group by responseCode
      """.stripMargin
    )

    resDF2.show()

    /**
      * 需求三： 获取访问次数超过N次的IP地址
      * 需求三额外：对IP地址进行限制，部分黑名单IP地址不统计
      */
    val blackIP = Array("200-55-104-193.dsl.prima.net.ar", "10.0.0.153", "208-38-57-205.ip.cal.radiant.net")
    val N = 10
    sql(
      s"""
         |select
         |ipAddress as ip,
         |count(1) as count
         |from log
         |where not (ipAddress in (${blackIP.map(ip => s"'$ip'").mkString(",")}))
         |group by ipAddress
         |having count > $N
      """.stripMargin).show()

    /**
      * 需求四：获取访问次数最多的前K个endpoint的值 ==> TopN
      */
    val K = 10
    sql(
      s"""
         |select
         |t.endpoint,
         |t.count
         |from(
         |select endpoint,
         |count(1) as count
         |from log
         |group by endpoint
         |) t
         |order by t.count desc
         |limit $K
      """.stripMargin).show()

    Thread.sleep(1000000)
    spark.stop()
  }
}
