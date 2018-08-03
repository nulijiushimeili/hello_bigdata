package spark03.sql

import org.apache.spark.sql.SparkSession

/**
  * create by nulijiushimeili on 2018-08-01
  */
object HiveOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val date = "2015-08-28"

    val sql_str =
      s"""insert overwrite table daily_visit partition (date='$date')
         |select count(distinct guid) uv,sum(pv) pv,
         |count(case when pv>=2 then sessionid else null end) second_num,
         |count(sessionid) visits from
         |(select ds date,sessionid,max(guid) guid,count(url) pv from track_log where ds='$date' and hour='18'
         |and length(url)>0
         |group by ds,sessionid) a
         |group by date
       """.stripMargin

    println(s"executing $sql_str ...")

    sql(sql_str).show()

    spark.stop()

  }
}
