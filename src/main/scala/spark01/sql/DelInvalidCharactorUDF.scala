package spark01.sql

import java.util.regex.Pattern

import org.apache.spark.sql.{SaveMode, SparkSession}
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-07
  */
object DelInvalidCharactorUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master(SparkProperties.master)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._

    spark.udf.register("del_not_date", (str: String, regex: String) => {
      val p = Pattern.compile(regex)
      val m = p.matcher(str.trim)
      if (!m.find()) {
        ""
      } else {
        str
      }
    })


    spark
      .read
      .table("db.tab")
      .createOrReplaceTempView("tab")

    spark.sql(
      s"""
         |select date,del_not_date(date,'\d{4}-\d{2}-\d{2} \d{2}:\d\d:\d\d') from tab
         """.stripMargin)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("new_table_name")

    spark.stop()
  }
}
